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
use std::{
    sync::{atomic::AtomicUsize, Arc},
    thread,
    time::Duration,
};
pub use util::*;

#[test]
fn slow_consumers() {
    let dropped_messages = Arc::new(AtomicUsize::new(0));
    let s = util::run_basic_server();
    let nc = nats::Options::with_user_pass("derek", "s3cr3t!")
        .error_callback({
            let dropped_messages = dropped_messages.clone();
            move |err| {
                if err.to_string()
                    == *"slow consumer detected for subscription id:1 with subject data.>"
                {
                    dropped_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        })
        .connect(s.client_url())
        .expect("could not connect");

    let sub = nc.subscribe("data.>").unwrap();
    sub.set_message_limits(100);

    let mut rng = rand::thread_rng();
    for i in 0..140 {
        let mut bytes = vec![0; 1024];
        rng.try_fill_bytes(&mut bytes).unwrap();
        nc.publish(format!("data.{}", i).as_str(), bytes).unwrap();
    }
    thread::sleep(Duration::from_millis(100));
    let mut i = 0;
    while i < 100 {
        sub.next();
        i += 1;
    }
    assert_eq!(
        sub.dropped_messages(),
        dropped_messages.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(sub.dropped_messages(), 40);
}
