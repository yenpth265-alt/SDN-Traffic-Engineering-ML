
#!/usr/bin/python3
"""
Leaf-Spine Multipath Topology for Mininet / Open vSwitch
- 8 Hosts: h1 - h8 (IPs: 10.0.0.1 - 10.0.0.8)
- 4 Edge Switches: e1 - e4 (DPIDs: 1 - 4)
- 2 Core/Spine Switches: c1, c2 (DPIDs: 5 - 6)
- Controller: RemoteController (Ryu)
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSKernelSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink

def create_topology():
    net = Mininet(
        controller=RemoteController,
        switch=OVSKernelSwitch,
        link=TCLink,
        autoSetMacs=True
    )

    info("*** Adding Remote Controller (Ryu)\n")
    net.addController('c0', controller=RemoteController, ip='127.0.0.1', port=6633)

    info("*** Adding Core/Spine Switches (c1, c2)\n")
    c1 = net.addSwitch('c1', protocols='OpenFlow13', dpid='0000000000000005')
    c2 = net.addSwitch('c2', protocols='OpenFlow13', dpid='0000000000000006')

    info("*** Adding Edge Switches (e1 - e4)\n")
    e1 = net.addSwitch('e1', protocols='OpenFlow13', dpid='0000000000000001')
    e2 = net.addSwitch('e2', protocols='OpenFlow13', dpid='0000000000000002')
    e3 = net.addSwitch('e3', protocols='OpenFlow13', dpid='0000000000000003')
    e4 = net.addSwitch('e4', protocols='OpenFlow13', dpid='0000000000000004')

    info("*** Adding Hosts (h1 - h8)\n")
    h1 = net.addHost('h1', ip='10.0.0.1/24')
    h2 = net.addHost('h2', ip='10.0.0.2/24')
    h3 = net.addHost('h3', ip='10.0.0.3/24')
    h4 = net.addHost('h4', ip='10.0.0.4/24')
    h5 = net.addHost('h5', ip='10.0.0.5/24')
    h6 = net.addHost('h6', ip='10.0.0.6/24')
    h7 = net.addHost('h7', ip='10.0.0.7/24')
    h8 = net.addHost('h8', ip='10.0.0.8/24')

    info("*** Configuring Links: Host – Edge Switches (10Mbps, 1ms delay, queue size 50)\n")
    net.addLink(h1, e1, port1=0, port2=1, bw=10, delay='1ms', max_queue_size=50)
    net.addLink(h2, e1, port1=0, port2=2, bw=10, delay='1ms', max_queue_size=50)
    
    net.addLink(h3, e2, port1=0, port2=1, bw=10, delay='1ms', max_queue_size=50)
    net.addLink(h4, e2, port1=0, port2=2, bw=10, delay='1ms', max_queue_size=50)

    net.addLink(h5, e3, port1=0, port2=1, bw=10, delay='1ms', max_queue_size=50)
    net.addLink(h6, e3, port1=0, port2=2, bw=10, delay='1ms', max_queue_size=50)

    net.addLink(h7, e4, port1=0, port2=1, bw=10, delay='1ms', max_queue_size=50)
    net.addLink(h8, e4, port1=0, port2=2, bw=10, delay='1ms', max_queue_size=50)

    info("*** Configuring Links: Edge – Core Switches (Port 3 -> c1, Port 4 -> c2, 10Mbps, 5ms delay)\n")
    net.addLink(e1, c1, port1=3, port2=1, bw=10, delay='5ms', max_queue_size=50)
    net.addLink(e1, c2, port1=4, port2=1, bw=10, delay='5ms', max_queue_size=50)

    net.addLink(e2, c1, port1=3, port2=2, bw=10, delay='5ms', max_queue_size=50)
    net.addLink(e2, c2, port1=4, port2=2, bw=10, delay='5ms', max_queue_size=50)

    net.addLink(e3, c1, port1=3, port2=3, bw=10, delay='5ms', max_queue_size=50)
    net.addLink(e3, c2, port1=4, port2=3, bw=10, delay='5ms', max_queue_size=50)

    net.addLink(e4, c1, port1=3, port2=4, bw=10, delay='5ms', max_queue_size=50)
    net.addLink(e4, c2, port1=4, port2=4, bw=10, delay='5ms', max_queue_size=50)

    info("*** Starting Mininet Network\n")
    net.start()

    info("*** Opening Mininet Command Line Interface (CLI)\n")
    CLI(net)

    info("*** Stopping Mininet Network\n")
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    create_topology()