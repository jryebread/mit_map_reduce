Going through the mit distributed systems class labs, since I didn't take a similar class at UCSD. \

https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

Lab 1 was fun, I learned that go channels are cool! Check out the super hacky approach in main/coordinator.go.

The implementation currently passes all tests, lot of print statements from me trying to figure out why the crash test wasnt passing. (turns out select statements block) main implementation of map reduce in coordinator.go and worker.go
