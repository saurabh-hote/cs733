# cs733-raft

This piece of work is an implementation of RAFT protocol. The reference paper can be found at https://ramcloud.stanford.edu/raft.pdf

# Features
* The code is divided into three different modules namely the Connection_Handler, the SharedLog and the KVStore.
* Current implementation does not incorporate the leader election process. The server which is run first will be the default leader of Raft. Others will be followers.
* All Client communication is handled by the Leader. The leader takes consensus from the majority of server replicas before committing the changes.
* If a client tries to communicate with a non-leader server then it gets "ERR_REDIRECT host port" message where host=ip-address/name of Leader and port=port# of leader.

# Team Members
Saurabh Hote 13305R008
Swapnil Gusani 133050001
Ramesh Gaikwad 13305R011

# Instructions: How to run
1.Clone the project using:
	git clone git@github.com:swapniel99/cs733-raft.git
2.Run the test script using following commands:
	cd cs733-raft
	go test

Thank you!
