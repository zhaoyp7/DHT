# Tutorial

This document collects learning advice and recommended resources.

## On Go syntax

It is a good idea to skim the official Go tutorial [A Tour of Go](https://go.dev/tour/list) first. Just get a general impression; you can look up specific syntax as you go while writing code.

The two topics you should focus on are [goroutine concurrent programming](https://chai2010.cn/advanced-go-programming-book/ch1-basic/ch1-06-goroutine.html) and [RPC (Remote Procedure Call)](https://chai2010.cn/advanced-go-programming-book/ch4-rpc/ch4-01-rpc-intro.html).

## On the test programs

The tests initialize a number of DHT nodes on your machine that exchange
information and maintain structure over the network, simulating how the system would behave across several distributed servers. You **should not** communicate through any channel other than the network (e.g. shared memory); your program **must** work correctly in a genuinely distributed setting.

The tests in this repository are **in-process Go tests** under `node/` (`go test ./node/...`), which spawn many nodes on `127.0.0.1`.

See the project [README](../README.md) for details.

## On DHT protocols

For an initial overview, the recommended survey [blog post](https://luyuhuang.tech/2020/03/06/dht-and-p2p.html) gives a good first understanding of the protocols.

For detailed technical specifics, read the two papers: [Chord](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf) and [Kademlia](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) (reading the papers is strongly encouraged).

Other supplementary references:
[Kademlia in Go](http://blog.notdot.net/tag/kademlia),
[an explanation of Chord](https://zhuanlan.zhihu.com/p/53711866),
[an explanation of Kademlia](http://xlattice.sourceforge.net/components/protocol/kademlia/specs.html#intro).

## On debugging

Avoid using a step-through debugger to debug the whole DHT. The DHT depends on timing, and single-stepping changes that timing, so the behavior under the debugger will differ from a normal run.

The recommended approach is to log each node's behavior using the [logrus](https://github.com/sirupsen/logrus) library and analyze the logs.

The generated log files can be very large (hundreds of MB), and many text editors cannot open or browse them comfortably. [Klogg](https://klogg.filimonov.dev/) is recommended for viewing such logs.

## On applications

Any creative idea is welcome. Some examples:

A fun [website](https://iknowwhatyoudownload.com/) that sniffs resource requests from IPs on the BitTorrent network.

A P2P file-sharing application: [BitTorrent](https://blog.jse.li/posts/torrent/#putting-it-all-together) and some explanatory material in this [blog post](https://www.cnblogs.com/LittleHann/p/6180296.html).
