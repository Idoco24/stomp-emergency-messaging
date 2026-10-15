#!/usr/bin/env python3
"""Real TCP and compiled C++ client checks, using only Python's standard library."""
import contextlib
import json
import os
from pathlib import Path
import queue
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
JAVA = os.environ.get('JAVA', shutil.which('java') or 'java')
CLIENT = Path(os.environ.get('STOMP_CLIENT', str(ROOT / 'client/bin/StompEMIClient')))


class Peer:
    def __init__(self, port):
        self.socket = socket.create_connection(('127.0.0.1', port), timeout=3)
        self.socket.settimeout(3)
        self.buffer = b''

    def send(self, command, headers=None, body=''):
        text = command + '\n' + ''.join(f'{k}:{v}\n' for k, v in (headers or {}).items()) + '\n' + body
        self.socket.sendall(text.encode('utf-8') + b'\0')

    def receive(self):
        while b'\0' not in self.buffer:
            data = self.socket.recv(4096)
            if not data:
                raise EOFError('Socket closed before a complete STOMP frame')
            self.buffer += data
        frame, self.buffer = self.buffer.split(b'\0', 1)
        text = frame.decode('utf-8')
        head, _, body = text.partition('\n\n')
        lines = head.splitlines()
        return lines[0], dict(line.split(':', 1) for line in lines[1:]), body

    def login(self, user, password='demo-password'):
        self.send('CONNECT', {'accept-version': '1.2', 'host': 'stomp.cs.bgu.ac.il', 'login': user, 'passcode': password})
        return self.receive()

    def subscribe(self, topic, subscription):
        self.send('SUBSCRIBE', {'destination': topic, 'id': str(subscription), 'receipt': 'joined'})
        assert self.receive()[0] == 'RECEIPT'

    def close(self):
        self.socket.close()

    def __enter__(self): return self
    def __exit__(self, *args): self.close()


@contextlib.contextmanager
def server(mode):
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    with tempfile.TemporaryFile(mode='w+t') as log:
        process = subprocess.Popen([JAVA, '-jar', str(ROOT / 'server/target/server-1.0.jar'), str(port), mode],
                                   stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 8
            while True:
                if process.poll() is not None:
                    log.seek(0)
                    raise AssertionError('Server exited during startup: ' + log.read())
                try:
                    probe = socket.create_connection(('127.0.0.1', port), timeout=.2)
                    probe.close()
                    break
                except OSError:
                    if time.monotonic() >= deadline: raise AssertionError('Server did not become ready')
                    time.sleep(.05)
            yield port
        finally:
            process.terminate()
            try: process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()


class ProtocolIntegration(unittest.TestCase):
    def test_reactor_closes_repeated_receipted_sessions(self):
        with server('reactor') as port:
            for iteration in range(20):
                with self.subTest(iteration=iteration), Peer(port) as peer:
                    self.assertEqual('CONNECTED', peer.login('repeat-user')[0])
                    peer.send('DISCONNECT', {'receipt': str(iteration)})
                    self.assertEqual(str(iteration), peer.receive()[1]['receipt-id'])
                    self.assertEqual(b'', peer.socket.recv(1))

    def test_two_clients_route_utf8_and_recipient_subscription_ids(self):
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port, Peer(port) as a, Peer(port) as b:
                self.assertEqual('CONNECTED', a.login('alice')[0]); self.assertEqual('CONNECTED', b.login('bob')[0])
                a.subscribe('/alerts', 7); b.subscribe('/alerts', 91)
                body = '  demo: שלום\n\n'
                a.send('SEND', {'destination': '/alerts', 'receipt': 'sent'}, body)
                own, received = a.receive(), b.receive()
                self.assertEqual(('MESSAGE', '7', body), (own[0], own[1]['subscription'], own[2]))
                self.assertEqual(('MESSAGE', '91', body), (received[0], received[1]['subscription'], received[2]))
                self.assertEqual('sent', a.receive()[1]['receipt-id'])

    def test_unsubscribe_and_disconnect_flush_receipts_and_close(self):
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port, Peer(port) as peer:
                self.assertEqual('CONNECTED', peer.login('alice')[0]); peer.subscribe('alerts', 3)
                peer.send('UNSUBSCRIBE', {'id': '3', 'receipt': 'left'})
                self.assertEqual('left', peer.receive()[1]['receipt-id'])
                peer.send('DISCONNECT', {'receipt': 'bye'})
                self.assertEqual('bye', peer.receive()[1]['receipt-id'])
                self.assertEqual(b'', peer.socket.recv(1))
                with Peer(port) as again: self.assertEqual('CONNECTED', again.login('alice')[0])

    def test_abrupt_disconnect_releases_username(self):
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port:
                with Peer(port) as first: self.assertEqual('CONNECTED', first.login('alice')[0])
                for attempt in range(20):
                    with Peer(port) as again:
                        if again.login('alice')[0] == 'CONNECTED': break
                    time.sleep(.05)
                else: self.fail('Disconnected user remained active')

    def test_wrong_password_duplicate_login_and_anonymous_commands(self):
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port, Peer(port) as alice:
                self.assertEqual('CONNECTED', alice.login('alice')[0])
                with Peer(port) as duplicate: self.assertEqual('ERROR', duplicate.login('alice')[0])
                with Peer(port) as wrong:
                    reply = wrong.login('alice', 'never-echo-this')
                    self.assertEqual('ERROR', reply[0]); self.assertNotIn('never-echo-this', reply[2])
                with Peer(port) as anonymous:
                    anonymous.send('SUBSCRIBE', {'destination': 'alerts', 'id': '1'})
                    self.assertEqual('ERROR', anonymous.receive()[0])

    def test_fragmented_crlf_frame_and_header_like_body(self):
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port, Peer(port) as peer:
                frame = b'\nCONNECT\r\naccept-version:1.2\r\nhost:stomp.cs.bgu.ac.il\r\nlogin:alice\r\npasscode:demo:colon\r\n\r\n\0'
                for start in range(0, len(frame), 3): peer.socket.sendall(frame[start:start+3])
                self.assertEqual('CONNECTED', peer.receive()[0]); peer.subscribe('alerts', 1)
                peer.send('SEND', {'destination': '/alerts'}, 'destination:/other\n')
                self.assertEqual('MESSAGE', peer.receive()[0])


class ClientProcess:
    def __init__(self):
        self.process = subprocess.Popen([str(CLIENT)], cwd=ROOT, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.lines = queue.Queue()
        self.output = []
        def read():
            for line in self.process.stdout:
                self.output.append(line); self.lines.put(line)
        self.reader = threading.Thread(target=read, daemon=True); self.reader.start()

    def command(self, line):
        self.process.stdin.write(line + '\n'); self.process.stdin.flush()

    def expect(self, text, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try: line = self.lines.get(timeout=max(.01, deadline-time.monotonic()))
            except queue.Empty: break
            if text in line: return
        raise AssertionError(f'Expected {text!r}, got: {"".join(self.output)}')

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try: self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill(); self.process.wait(); raise AssertionError('Client failed to stop at stdin EOF')
        self.reader.join(timeout=1)
        self.process.stdout.close()
        return self.process.returncode


class CppClientIntegration(unittest.TestCase):
    def test_cpp_report_summary_unsubscribe_logout_and_eof(self):
        self.assertTrue(CLIENT.is_file(), 'Build the C++ client before running integration tests')
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port, tempfile.TemporaryDirectory() as directory:
                report = Path(directory)/'events.json'; summary = Path(directory)/'summary.txt'
                report.write_text(json.dumps({'channel_name':'alerts','events':[{
                    'event_name':'Synthetic: exercise', 'city':'Demo City', 'date_time':1735689600,
                    'description':'A simulated event: no real emergency.',
                    'general_information':{'active':True,'forces_arrival_at_scene':False}}]}))
                client = ClientProcess()
                try:
                    client.command(f'login 127.0.0.1:{port} alice demo-password'); client.expect('Login successful')
                    client.command('join alerts'); client.expect('Joined channel alerts')
                    client.command('report ' + str(report)); client.expect('reported')
                    # A following receipt is a delivery barrier for messages already queued to this session.
                    client.command('join barrier'); client.expect('Joined channel barrier')
                    client.command(f'summary alerts alice {summary}'); client.expect('Summary written')
                    text = summary.read_text()
                    self.assertIn('Total: 1', text); self.assertIn('Synthetic: exercise', text)
                    self.assertIn('A simulated event: no real emergency.', text)
                    other = Path(directory)/'other.txt'
                    client.command(f'summary alerts ali {other}'); client.expect('Summary written')
                    self.assertIn('Total: 0', other.read_text())
                    client.command('exit alerts'); client.expect('Exited from alerts')
                    client.command('logout'); client.expect('Logged out')
                    self.assertEqual(0, client.close())
                finally:
                    if client.process.poll() is None: client.close()

    def test_cpp_bad_login_is_reported_without_aborting(self):
        for mode in ('tpc', 'reactor'):
            with self.subTest(mode=mode), server(mode) as port, Peer(port) as owner:
                self.assertEqual('CONNECTED', owner.login('alice')[0])
                client = ClientProcess()
                try:
                    client.command(f'login 127.0.0.1:{port} alice wrong-password'); client.expect('ERROR FROM THE SERVER')
                    self.assertEqual(0, client.close())
                finally:
                    if client.process.poll() is None: client.close()

    def test_cpp_receives_frame_split_beyond_poll_timeout(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0)); listener.listen()
            listener.settimeout(5)
            failures = []
            def fake_server():
                try:
                    with listener.accept()[0] as connection:
                        connection.settimeout(5)
                        request = b''
                        while b'\0' not in request: request += connection.recv(1024)
                        connection.sendall(b'CONNE')
                        time.sleep(.15)  # longer than the client's 50ms polling window
                        connection.sendall(b'CTED\nversion:1.2\n\n\0')
                        while connection.recv(1024): pass
                except Exception as error: failures.append(str(error))
            worker = threading.Thread(target=fake_server, daemon=True); worker.start()
            client = ClientProcess()
            try:
                client.command(f'login 127.0.0.1:{listener.getsockname()[1]} alice demo')
                client.expect('Login successful')
                self.assertEqual(0, client.close())
            finally:
                if client.process.poll() is None: client.close()
                worker.join(timeout=6)
            self.assertFalse(worker.is_alive()); self.assertEqual([], failures)


if __name__ == '__main__': unittest.main(verbosity=2)
