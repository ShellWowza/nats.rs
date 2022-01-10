// Copyright 2020-2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod util;
use rand::prelude::*;
use std::{str::from_utf8, thread, time::Duration};
pub use util::*;

#[test]
fn slow_consumers() {
    let s = util::run_basic_server();
    // let nc = nats::connect(&s.client_url()).expect("could not connect");
    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback(move |err| println!("error: {}", err))
        .reconnect_callback(|| println!("reconnected"))
        .disconnect_callback(|| println!("disconnected"))
        // .connect(&s.client_url())
        .connect(s.client_url())
        .expect("could not connect");

    let sub = nc.subscribe("data.>").unwrap();
    sub.set_message_limits(100);
    thread::spawn({
        let nc = nc.clone();
        move || {
            let mut rng = rand::thread_rng();
            for i in 0..120 {
                if i % 1000 == 0 {
                    println!("another batch sent, at: {}", i);
                }
                let mut bytes = Vec::with_capacity(1024 * 1024);
                bytes.resize(1024, 0);
                rng.try_fill_bytes(&mut bytes).unwrap();
                // thread::sleep(Duration::from_millis(1));
                nc.publish(format!("data.{}", i).as_str(), bytes).unwrap();
            }
        }
    });
    let mut i = 0;
    thread::sleep(Duration::from_millis(100));
    for m in sub.iter() {
        // let mi: i32 = from_utf8(&m.data).unwrap().parse().unwrap();
        // assert_eq!(i, mi);
        i += 1;
        if i == 100 {
            break;
        }
    }
    assert!(sub.dropped_messages() > 1);
    println!("dropped messages {}", sub.dropped_messages());
}
