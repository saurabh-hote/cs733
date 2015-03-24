#CS733 - Engineering a cloud

Saurabh Hote 13305R008

Note: Not ready for evaluation.
Coded for leader election part. But implementation for logging part is not done yet. Also, need to code the test cases.
I am planning to complete the implementation over the weekend.

# Description
This piece of work is an implementation of Raft protocol. The reference paper can be found at https://ramcloud.stanford.edu/raft.pdf

# Features
* The code is divided into three different modules namely the Connection_Handler, the SharedLog and the KVStore.
* Current implementation does not incorporate the leader election process. The server which is run first will be the default leader of Raft. Others will be followers.
* All Client communication is handled by the Leader. The leader takes consensus from the majority of server replicas before committing the changes.
* If a client tries to communicate with a non-leader server then it gets "ERR_REDIRECT host port" message where host=ip-address/name of Leader and port=port# of leader.


# Instructions: How to run
1.Obtain a copy of the project using:	go get github.com/saurabh-hote/cs733

2.Change directory to cs733-raft : cd $GOPATH/src/github.com/saurabh-hote/cs733/assignment-3/

3.Run the test script using following commands: 
					go run server.go

Thank you!
 