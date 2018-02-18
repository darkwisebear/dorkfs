extern crate clap;
extern crate failure;
extern crate fuse_mt;
extern crate libc;
extern crate time;
#[macro_use] extern crate log;
extern crate env_logger;

mod core;

fn parse_arguments() -> clap::ArgMatches<'static> {
    use clap::{App, Arg};

    App::new("dorkfs")
        .arg(Arg::with_name("cachedir")
            .takes_value(true)
            .required(true)
            .help("Directory where the cached contents shall be stored"))
        .arg(Arg::with_name("mountpoint")
            .takes_value(true)
            .required(true)
            .help("Mountpoint that shows the checked out contents"))
        .get_matches()
}

fn main() {
    env_logger::init();

    let args = parse_arguments();
    let cachedir = args.value_of("cachedir").expect("cachedir arg not set!");
    let mountpoint = args.value_of("mountpoint").expect("mountpoint arg not set!");
    let dorkfs = core::DorkFs::new(cachedir).unwrap();
    dorkfs.mount(mountpoint).unwrap();
}
