import paramiko
from scp import SCPClient
import time
class Connection:
    def __init__(self, ip, user):
        self.addr = ip
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(ip, username=user)
        self.scp_client = SCPClient(self.ssh_client.get_transport())
        self.sftp_client = self.ssh_client.open_sftp()
        self.long_running = []

    # Synchronously executes a command
    def exec_command(self, command, sync=False):

        if sync:
            stdin, stdout, stderr = self.ssh_client.exec_command(command)

            if stdout.channel.recv_exit_status() == 1:
                print(f"ERROR: Out: {stdout.read().decode()}, stderr: {stderr.read().decode()}")

        else:
            chan = self.ssh_client.get_transport().open_session()
            chan.get_pty()
            chan.exec_command(command)
            self.long_running.append(chan)

    # Wait for the complete execution of some commands
    def wait_command(self):
        print(f"Len: {self.long_running}")
        for chan in self.long_running:
            exit_code = chan.recv_exit_status()
            print("Process finished with", exit_code)

        self.long_running.clear()

    def stop(self, timeout = 5):
        for chan in self.long_running:
            try:
                # Send SIGINT to the process group
                chan.send('\x03')  # Ctrl+C / SIGINT

                # Wait for the channel to close within the timeout
                start = time.time()
                while not chan.closed and (time.time() - start) < timeout:
                    time.sleep(0.1)

                # If still running after timeout, hard stop
                if not chan.closed:
                    chan.close()

            except Exception:
                chan.close()

        self.long_running.clear()

    def close(self):

        for chan in self.long_running:
            chan.close()

        self.long_running.clear()

        self.scp_client.close()
        self.sftp_client.close()
        self.ssh_client.close()
