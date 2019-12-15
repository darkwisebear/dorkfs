use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    fmt::{self, Display, Debug, Formatter},
    mem::replace,
    pin::Pin,
    collections::vec_deque::{self, VecDeque}
};

use difference::{self, Difference};
use failure::Fallible;
use futures::{
    task::{Context, Poll},
    prelude::*,
};
use pin_project::{project, pin_project};

use crate::{
    overlay::{WorkspaceFileStatus, WorkspaceController},
};

struct HistoryItem<'a, T>(&'a VecDeque<T>);

impl<'a, T> HistoryItem<'a, T> {
    fn current(&self) -> &'a T {
        self.0.back().unwrap()
    }

    fn iter_history(&self) -> vec_deque::Iter<'a, T> {
        self.0.iter()
    }
}

struct LookbackStreamingIterator<T, I> where I: Iterator<Item=T>,
                                             T: Clone {
    inner: I,
    history: VecDeque<T>,
    history_size: usize
}

impl<T, I> LookbackStreamingIterator<T, I> where I: Iterator<Item=T>,
                                                 T: Clone {
    fn new(inner: I, history_size: usize) -> Self {
        Self {
            inner,
            history: VecDeque::with_capacity(history_size+1),
            history_size: history_size+1
        }
    }

    fn next(&mut self) -> Option<HistoryItem<T>> {
        self.inner.next()
            .map(move |item| {
                if self.history.len() >= self.history_size {
                    self.history.pop_front();
                }

                self.history.push_back(item);

                HistoryItem(&self.history)
            })
    }
}

#[derive(Debug)]
pub struct FileDifference {
    pub differences: Vec<Difference>,
    pub path: PathBuf
}

impl Display for FileDifference {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        writeln!(fmt, "{}", self.path.display())?;

        const CONTEXT_SIZE: usize = 3;

        let lines_iter = self.differences.iter().flat_map(|diff| {
            let (prefix, line) = match diff {
                Difference::Add(line) => ('+', line.as_str()),
                Difference::Rem(line) => ('-', line.as_str()),
                Difference::Same(line) => (' ', line.as_str()),
            };

            line.split('\n').map(move |line| (prefix, line))
        });
        let mut lines_w_history = LookbackStreamingIterator::new(
            lines_iter, CONTEXT_SIZE);

        let mut current_context_size = 0;
        let mut inside = false;

        while let Some(line_w_history) = lines_w_history.next() {
            let &(prefix, line) = line_w_history.current();
            if inside {
                write!(fmt, "\n{} {}", prefix, line)?;
                if prefix == ' ' {
                    current_context_size -= 1;
                    if current_context_size == 0 {
                        inside = false;
                    }
                } else {
                    current_context_size = CONTEXT_SIZE
                }
            } else {
                match prefix {
                    ' ' => (),
                    _ => {
                        for &(prefix, line) in line_w_history.iter_history() {
                            write!(fmt, "\n{} {}", prefix, line)?;
                        }

                        inside = true;
                        current_context_size = CONTEXT_SIZE;
                    }
                }
            }
        }

        Ok(())
    }
}

#[pin_project]
enum WSDiffIterState<F> {
    NextWsStatus,
    CurrentPoll(PathBuf, #[pin] F),
    End
}

impl<F> Debug for WSDiffIterState<F> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            WSDiffIterState::NextWsStatus => f.write_str("NextWsStatus"),
            WSDiffIterState::CurrentPoll(ref path, _)
                => write!(f, "CurrentPoll on {}", path.display()),
            WSDiffIterState::End => f.write_str("End"),
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct WorkspaceDiffIter<'a, R> where R: WorkspaceController<'a> {
    ws_status: Vec<WorkspaceFileStatus>,
    repo: Arc<RwLock<R>>,
    #[pin]
    state: WSDiffIterState<R::DiffFuture>
}

impl<'b, R> WorkspaceDiffIter<'b, R> where for<'a> R: WorkspaceController<'a> {
    pub fn new(repo: Arc<RwLock<R>>) -> Fallible<Self> {
        let ws_status = {
            let repo = repo.read().unwrap();
            let status_iter = repo.get_status()?;
            status_iter.collect::<Result<Vec<WorkspaceFileStatus>, _>>()?
        };

        Ok(Self {
            ws_status,
            repo,
            state: WSDiffIterState::NextWsStatus
        })
    }
}

impl<'a, R> Stream for WorkspaceDiffIter<'a, R> where R: WorkspaceController<'a>+'a {
    type Item = Fallible<FileDifference>;

    #[project]
    fn poll_next(self: Pin<&mut Self>, context: &mut Context) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        #[project]
        let (result, next_state) = match me.state.as_mut().project() {
            WSDiffIterState::NextWsStatus => {
                if let Some(WorkspaceFileStatus(path, _)) = me.ws_status.pop() {
                    let poll = me.repo.write().unwrap().get_diff(path.as_path());
                    let next_state = WSDiffIterState::CurrentPoll(path, poll);
                    context.waker().wake_by_ref();
                    (Poll::Pending, Some(next_state))
                } else {
                    (Poll::Ready(None), Some(WSDiffIterState::End))
                }
            }

            WSDiffIterState::CurrentPoll(path, poll) => {
                if let Poll::Ready(differences) = poll.poll(context)? {
                    let next_state = WSDiffIterState::NextWsStatus;
                    let result = Poll::Ready(Some(Ok(FileDifference {
                        path: replace(path, PathBuf::new()),
                        differences
                    })));
                    (result, Some(next_state))
                } else {
                    (Poll::Pending, None)
                }
            }

            WSDiffIterState::End => (Poll::Ready(None), None)
        };

        if let Some(next_state) = next_state {
            // This is ok as the old future has already been used and the new one is still unused.
            // Everything else is Unpin
            unsafe {
                *me.state.get_unchecked_mut() = next_state;
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use super::FileDifference;
    use std::path::PathBuf;
    use difference::{Difference, Changeset};

    fn check_file_difference(original_text: &str, modified_text: &str, file_name: &str,
                             file_difference: &str) {
        let changeset = Changeset::new(original_text, modified_text, "\n");

        let fd = FileDifference {
            differences: changeset.diffs,
            path: PathBuf::from(file_name)
        };

        assert_eq!(&fd.to_string(), file_difference);
    }

    #[test]
    fn simple_difference() {
        let original_text = r"This is a test
String
Blubb
Weihnachten is today
not";
        let modified_text = r"This is a test
String
Tilt
Weihnachten is today
not";

        let truth = r"README

  This is a test
  String
- Blubb
+ Tilt
  Weihnachten is today
  not";
        check_file_difference(original_text, modified_text, "README", truth);
    }

    #[test]
    fn diff_with_missing_end() {
        let original_text = r"This is a test
String
Blubb
Weihnachten is today
not
in
that
line";
        let modified_text = r"This is a test
String
Tilt
Weihnachten is today
not
in
that
line";

        let truth = r"README

  This is a test
  String
- Blubb
+ Tilt
  Weihnachten is today
  not
  in";
        check_file_difference(original_text, modified_text, "README", truth);
    }

    #[test]
    fn diff_with_missing_start() {
        let original_text = r"This is a test
with
a
long
start
String
Blubb
Weihnachten is today
not";
        let modified_text = r"This is a test
with
a
long
start
String
Tilt
Weihnachten is today
not";

        let truth = r"README

  long
  start
  String
- Blubb
+ Tilt
  Weihnachten is today
  not";
        check_file_difference(original_text, modified_text, "README", truth);
    }

    #[test]
    fn diff_with_two_adjacent_hunks() {
        let original_text = r"This is a test
with
a
long
start
String
Blubb
some
lines
in
between
the
hunks
Original
and
some
more
text
Weihnachten is today
not";
        let modified_text = r"This is a test
with
a
long
start
String
Tilt
some
lines
in
between
the
hunks
change
and
some
more
text
Weihnachten is today
not";

        let truth = r"README

  long
  start
  String
- Blubb
+ Tilt
  some
  lines
  in
  between
  the
  hunks
- Original
+ change
  and
  some
  more";

        check_file_difference(original_text, modified_text, "README", truth);
    }

    #[test]
    fn diff_with_two_disjoint_hunks() {
        let original_text = r"This is a test
with
a
long
start
String
Blubb
some
more
lines
in
between
the
hunks
Original
and
some
more
text
Weihnachten is today
not";
        let modified_text = r"This is a test
with
a
long
start
String
Tilt
some
more
lines
in
between
the
hunks
change
and
some
more
text
Weihnachten is today
not";

        let truth = r"README

  long
  start
  String
- Blubb
+ Tilt
  some
  more
  lines
  between
  the
  hunks
- Original
+ change
  and
  some
  more";

        check_file_difference(original_text, modified_text, "README", truth);
    }
}
