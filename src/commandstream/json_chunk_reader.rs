use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    future::Future,
};

use tokio::{
    io::{AsyncRead, AsyncReadExt},
    stream::Stream,
};

async fn read_json_chunk(mut reader: impl AsyncRead+Unpin) -> io::Result<Box<[u8]>> {
    let mut curly = 0usize;
    let mut angle = 0usize;
    let mut buf = Vec::with_capacity(1024);

    loop {
        let mut start_buf = [0u8; 16];
        let num_bytes = reader.read(&mut start_buf).await?;
        if num_bytes > 0 {
            if let Some((pos, _)) = (&start_buf[..num_bytes]).iter()
                .enumerate()
                .find(|&(_, c)| *c == b'{' || *c == b'[') {
                buf.extend_from_slice(&start_buf[pos..num_bytes]);
                break;
            }
        } else {
            return Err(io::ErrorKind::InvalidData.into());
        }
    }

    let mut slice_start = 0usize;
    let mut slice_end = buf.len();
    loop {
        for (pos, c) in (&buf[slice_start..slice_end]).iter().enumerate() {
            match c {
                b'{' => curly+=1,
                b'}' => curly = curly.checked_sub(1)
                    .ok_or(io::Error::from(io::ErrorKind::InvalidData))?,
                b'[' => angle+=1,
                b']' => angle = angle.checked_sub(1)
                    .ok_or(io::Error::from(io::ErrorKind::InvalidData))?,
                _ => ()
            }

            if angle == 0 && curly == 0 {
                buf.truncate(slice_start+pos+1);
                return Ok(buf.into_boxed_slice());
            }
        }

        let bytes_read = reader.read_buf(&mut buf).await?;
        if bytes_read == 0 {
            return Err(io::ErrorKind::InvalidData.into())
        } else {
            slice_start = slice_end;
            slice_end = slice_start+bytes_read;
        }
    }
}

pub(super) struct JsonChunkReader {
    chunker: Option<Pin<Box<dyn Future<Output=io::Result<Box<[u8]>>>+Send>>>
}

impl JsonChunkReader {
    pub(super) fn new<R: AsyncRead+Unpin+Send+'static>(reader: R) -> Self {
        Self { chunker: Some(Box::pin(read_json_chunk(reader))) }
    }
}

impl Stream for JsonChunkReader {
    type Item = io::Result<Box<[u8]>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(chunker) = self.chunker.as_mut() {
            let chunker = chunker.as_mut();
            match chunker.poll(cx) {
                Poll::Ready(item) => {
                    self.chunker = None;
                    Poll::Ready(Some(item))
                },
                Poll::Pending => Poll::Pending
            }
        } else {
            Poll::Ready(None)
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use tokio;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn read_simple_json_obejct() {
        let test_object = r#"{ "test": "object" }"#;
        let result = super::read_json_chunk(test_object.as_bytes()).await.unwrap();
        assert_eq!(&*result, test_object.as_bytes());
    }

    #[tokio::test]
    async fn read_chunked_json_object() {
        let test_obj_part1 = r#"{ "test": "o"#;
        let test_obj_part2 = r#"bject" }"#;
        let reader = test_obj_part1.as_bytes().chain(test_obj_part2.as_bytes());
        let result = super::read_json_chunk(reader).await.unwrap();
        assert_eq!(&*result, r#"{ "test": "object" }"#.as_bytes());
    }

    #[tokio::test]
    async fn read_obj_with_leading_and_trailing_garbage() {
        let test_obj_part1 = r#"asbkds; hjdd{ "test": "o"#;
        let test_obj_part2 = r#"bject" }cksd;jdbhfbd}cnjsads]bjsb"#;
        let reader = test_obj_part1.as_bytes().chain(test_obj_part2.as_bytes());
        let result = super::read_json_chunk(reader).await.unwrap();
        assert_eq!(&*result, r#"{ "test": "object" }"#.as_bytes());
    }

    #[tokio::test]
    async fn fail_on_incomplete_obj() {
        let test_object = r#"{ "test": "object" "#;
        let result = super::read_json_chunk(test_object.as_bytes()).await.unwrap_err();
        assert_eq!(result.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn fail_on_empty_input() {
        let result = super::read_json_chunk("".as_bytes()).await.unwrap_err();
        assert_eq!(result.kind(), io::ErrorKind::InvalidData);
    }
}
