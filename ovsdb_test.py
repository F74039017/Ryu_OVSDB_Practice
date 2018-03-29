# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types

from ryu.lib import hub
from ryu.lib.packet import ipv4, udp
from json import dump, dumps
from ovs.vlog import Vlog

from ryu.app.rest_router import ipv4_text_to_int, ipv4_int_to_text
from ryu.topology.api import get_switch, get_link, get_host
from ryu.services.protocols.ovsdb import api as ovsdb
from ryu.services.protocols.ovsdb import event as ovsdb_event
import pyjsonrpc
import netifaces as ni


'''
Pratice QoS with OVSDB

This app works with SimpleSwitch13, which use L2 switch forwarding logic.
Only 'udp' traffic can be programmed and pushed to specific queue accroding to 'dst_ip' and 'dst_port' currently.
This app uses only one qos record in ovsdb, and qos column of all ports will reference to this qos record.
With rpc.py, we can create or modify ovsdb queues. All created queues will be bound to the qos.

Usage: ryu-manager ryu.app.simple_switch_13 ryu.app.ovsdb_test

Experiment:

    1. Start ryu app
        $ ryu-manager ryu.app.simple_switch_13 ryu.app.ovsdb_test

    2. Create simple mininet topology
        $ sudo mn --topo single,2 --mac --controller remote --switch ovsk,protocols=OpenFlow13

    3. Start rpc.py
        $ python rpc.py <controller ip>

    4. Use rpc.py to configure queue in ovsdb
        Command> 1 10.0.0.2 5001 3000000
        Command> 2 10.0.0.2 5002 7000000

    5. Execute two iperf servers in 10.0.0.2 nodes, and listen on port 5001, 5002 respectively
        $ iperf -u -s -p 5001
        $ iperf -u -s -p 5002

    6. Execute two iperf clients in 10.0.0.1 nodes, and send udp traffic to those servers
        $ iperf -u -c 10.0.0.2 -b 10M -t 10 -p 5001
        $ iperf -u -c 10.0.0.2 -b 10M -t 10 -p 5002

    7. Verify the result
        5001> 3.42 Mbits/sec
        5002> 6.97 Mbits/sec

'''


# Const.
QOS_MAX_RATE = '10000000'
IFACE_NAME = 'eno1'
RPC_PORT = 5656
FLOW_PRIORITY = 200 # this should be higher than SimpleSwitch13

class OVSDB_TEST(app_manager.RyuApp):

    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(OVSDB_TEST, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.rpc = RPCServer(app=self)

        # OVS log settings. Enable error level logging for ovs related module.
        Vlog.set_level('any', 'any', 'err')
        Vlog.init() 

        self.system_id = None # assume only one ovsdb
        self.qos_uuid = None
        self.qos_rule = [] # [(dst_ip, dst_port, qid)...]
        self.isOvSConnect = False

        self.dps = None # reference to switches app. Do not modify this variable!!
        hub.spawn(self.get_dps_ref)

        # mimic SimpleSwitch13
        # XXX: Although this will cause duplicated code, we can't guarantee that SimpleSiwtch13 processes packet_in before this app.
        self.mac_to_port = {}
        self.ip_to_mac = {}

    ####################################
    #  GET REFERENCE FROM OTHER BRICK  #
    ####################################

    def get_dps_ref(self):
        while True:
            brick = app_manager.lookup_service_brick('switches')
            if brick:
                self.dps = brick.dps
                print 'Get dps reference from switches app'
                break
            hub.sleep(100)

    ##############################
    #  OVSDB CONFIGURE FUNCTION  #
    ##############################

    def create_qos(self):

        qos_table = ovsdb.get_table(self, self.system_id, 'QoS') 
        if len(qos_table.rows.values())>0:
            self.qos_uuid = qos_table.rows.values()[0].uuid
            print "\033[1;32;49mUse old qos: {}\033[0;37;49m".format(self.qos_uuid)
            return True

        def _create_qos(tables, insert):

            # insert new qos 
            qos = insert(tables['QoS']) 
            qos.other_config = {'max-rate': QOS_MAX_RATE}
            qos.type = "linux-htb"

            # return local uuid(s), this will create a map in rep.insert_uuids
            # {local uuid: real uuid in ovsdb}
            return qos.uuid 

        req = ovsdb_event.EventModifyRequest(self.system_id, _create_qos)
        rep = self.send_request(req)
        self.qos_uuid = rep.insert_uuids.values()[0] # insert_uuids is a uuid map
        print "\033[1;32;49mCreate qos: {}\033[0;37;49m".format(self.qos_uuid)

        if rep.status != 'success':
            return False
        return True

    # FIXME: This will bind all ports -> qos, even the port was binded before
    def bind_qos(self):

        qos_uuid = self.qos_uuid

        # get port table
        ports = ovsdb.get_table(self, self.system_id, 'Port') 

        def _bind_qos(tables, insert):

            # Bind port -> qos
            for row in ports.rows.values():
                row.qos = qos_uuid

            print 'Bind ovsdb: port -> qos'
            return None

        req = ovsdb_event.EventModifyRequest(self.system_id, _bind_qos)
        rep = self.send_request(req)

        if rep.status != 'success':
            return False
        return True

    def prepare_and_bind_queue(self, queue_id, min_rate):
    
        # get qos row by uuid
        # XXX: In this app, only one qos is used
        qos_table = ovsdb.get_table(self, self.system_id, 'QoS')
        qos = qos_table.rows.get(self.qos_uuid, None)

        if qos is None:
            print "Error: qos row doesn't esxit!!"
            return

        # try to get old queue by qid
        old_queue = None
        try:
            old_queue_uuid = qos.queues[int(queue_id)].uuid
            # get Row of the old_queue
            queue_table = ovsdb.get_table(self, self.system_id, 'Queue')
            old_queue = queue_table.rows.get(old_queue_uuid) # Row obj
        except:
            pass


        def _prepare_and_bind_queue(tables, insert):

            if old_queue:
                queue = old_queue
                print "\033[1;32;49mOld queue min-rate is changed to {0}: {1}\033[0;37;49m".format(min_rate, queue.uuid)
            else:
                queue = insert(tables['Queue'])

            queue.other_config = {'min-rate': str(min_rate)} 

            # Bind qos -> queue
            # Need to assign the column back to Row obj; otherwise, it won't modify the ovsdb
            tmp = qos.queues
            tmp[int(queue_id)] = queue.uuid
            qos.queues = tmp

            print 'Bind ovsdb: qos -> queues'
            return queue.uuid

        req = ovsdb_event.EventModifyRequest(self.system_id, _prepare_and_bind_queue)
        rep = self.send_request(req)
        if rep.insert_uuids.values()[0]:
            print "\033[1;32;49mCreate queue: {}\033[0;37;49m".format(rep.insert_uuids.values()[0])

        if rep.status != 'success':
            return False
        return True

    ###########################
    #  RYU EVENTS & ADD FLOW  # 
    ###########################

    @set_ev_cls(ovsdb_event.EventNewOVSDBConnection)
    def handle_new_ovsdb_connection(self, ev):
        print "\033[1;32;49mOVSDB connected: system id = {}\033[0;37;49m".format(ev.system_id)
        self.system_id = ev.system_id
        self.isOvSConnect = True

        # create one qos entry
        self.create_qos()
        self.bind_qos()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):

        # XXX: Sometimes, this is too fast that the ovsdb hasn't connected to controller yet
        # bind all ports to qos entry
        self.bind_qos()

    def add_qos_rule_flow(self, datapath, dst_ip, dst_port, qid, buffer_id=None):

        dpid = datapath.id
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()

        # only match by dst. info.
        match.set_ipv4_dst(ipv4_text_to_int(dst_ip.encode('utf8')))
        match.set_udp_dst(int(dst_port))
        match.set_dl_type(0x800) # prereq of ipv4
        match.set_ip_proto(0x11) # prereq of udp

        # SimpleSwitch13 forwarding logic
        if self.ip_to_mac.get(dst_ip, None) in self.mac_to_port[dpid]:
            print "\033[1;34;49mAdd QoS flow: dst_ip {}, dst_port {} => Qid {}\033[0;37;49m".format(dst_ip, dst_port, qid)
            out_port = self.mac_to_port[dpid][self.ip_to_mac[dst_ip]]
        else:
            return

        # push to queue before output
        actions = [parser.OFPActionSetQueue(int(qid)), parser.OFPActionOutput(out_port)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=FLOW_PRIORITY, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=FLOW_PRIORITY,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):

        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        ip = pkt.get_protocol(ipv4.ipv4)

        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            # ignore lldp packet
            return

        dst_mac = eth.dst
        src_mac = eth.src

        # mimic SimpleSwitch13
        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src_mac] = in_port

        if not ip:
            return

        dst_ip = ip.dst
        src_ip = ip.src

        self.ip_to_mac[dst_ip] = dst_mac # dst_ip is string
        self.ip_to_mac[src_ip] = src_mac

        # check if there is an qos_rule matching
        for _dst_ip, _dst_port, _qid in self.qos_rule:
            if _dst_ip == dst_ip:
                self.add_qos_rule_flow(datapath, _dst_ip, _dst_port, _qid)


def MakeHandlerClass(kwargs):
    class CustomHandler(pyjsonrpc.HttpRequestHandler):

        # FIXME: RPC won't modify switches that connected to the controller later

        # Wait ovsdb connect to the controller then add qos rule to the switches connecting to the controller
        # QoS Rule: Create an queue and set qos's queues -> queue

        @pyjsonrpc.rpcmethod
        def add_qos_rule(self, cmd):

            qid = cmd['qid']
            dst_ip = cmd['dst_ip']
            dst_port = cmd['dst_port']
            min_rate = cmd['min_rate']

            app = kwargs['app']

            if app.isOvSConnect and app.qos_uuid:
                # debug
                print dumps(cmd, sort_keys = True, indent = 4)
    
                app.prepare_and_bind_queue(qid, min_rate)
                app.qos_rule.append((dst_ip, dst_port, qid))
                # try to add qos rule if ip appeared in packet_in
                for dp in app.dps.values():
                    app.add_qos_rule_flow(dp, dst_ip, dst_port, qid)

                return "OK\n"
            else:
                print "OVSDB not connected or qos hasn't been created"
                return "Fail: OVSDB not connected or null qos uuid\n"

    return CustomHandler

class RPCServer():
    def __init__(self, **kwargs):
        HandlerClass = MakeHandlerClass(kwargs=kwargs)
        ni.ifaddresses(IFACE_NAME)
        self.ip = ni.ifaddresses(IFACE_NAME)[ni.AF_INET][0]['addr']
        self.httpServer = pyjsonrpc.ThreadingHttpServer(server_address = (self.ip, RPC_PORT), RequestHandlerClass = HandlerClass)
        self.startThread = hub.spawn(self.start)

    def start(self):
        print 'Start RPC Server'
        print '\033[1;32;49mURL: {0}:{1}\033[0;37;49m'.format(self.ip, RPC_PORT)
        self.httpServer.serve_forever()
