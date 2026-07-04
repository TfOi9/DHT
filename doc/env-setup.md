# Environment Setup

The configuration below has been tested on Ubuntu 22.04.

**Note: the test programs do not run correctly on WSL 1.** Follow the Microsoft documentation [Check which version of WSL you are running](https://learn.microsoft.com/en-us/windows/wsl/install#check-which-version-of-wsl-you-are-running); if you are on WSL 1, please [upgrade from WSL 1 to WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install#upgrade-version-from-wsl-1-to-wsl-2).

## Installing Go

This project requires Go 1.18 or newer.

Run:

```bash
sudo apt install golang-go
```

After installation, check the Go version:

```bash
go version
```

Configure the [Go module proxy](https://goproxy.cn/) to speed up module downloads:

```bash
go env -w GO111MODULE=on
go env -w GOPROXY=https://goproxy.cn,direct
```

### Alternative method

Remove the old version of Go:

```bash
sudo rm -rf /usr/local/go
```

Download the Go archive (you can get the latest download link from the
[Go website](https://go.dev/dl/)):

```bash
curl -LO "https://go.dev/dl/go1.20.5.linux-amd64.tar.gz"
```

Extract it into `/usr/local`:

```bash
sudo tar -C /usr/local -xzf go1.20.5.linux-amd64.tar.gz
```

Add `/usr/local/go/bin` to your `PATH`.

If you use bash:

```bash
echo 'export PATH="$PATH:/usr/local/go/bin"' >> ~/.bashrc
```

If you use zsh:

```bash
echo 'export PATH="$PATH:/usr/local/go/bin"' >> ~/.zshrc
```

Restart your terminal so the environment variable takes effect.

## Setting up VS Code

> VS Code is the recommended editor. You may also use other IDEs such as GoLand, but you will need to solve the environment configuration yourself.

> If you work on a virtual machine or server, the VS Code Remote - SSH extension is recommended for developing on it.

Install the [VS Code Go extension](https://marketplace.visualstudio.com/items?itemName=golang.go).

After installation you will be prompted several times to install missing tools,
with messages such as:

```plain
The "gopls" command is not available. Run "go install -v golang.org/x/tools/gopls@latest" to install.
```

Choose Install for all of them.

## Building the project

From the project root, run:

```bash
go build -o dht .
```

If the build succeeds, an executable named `dht` is produced in the current directory, which means your environment is set up correctly.

## Running the tests

There are two test layers (see the [README](../README.md) for details).

The in-process tests run many nodes on `127.0.0.1`:

```bash
go test ./node/...
```

The tests take a while. On success you will see output similar to:

```plain
Basic test passed.
Force quit test passed.
Quit & Stabilize test passed.
ok      dht/node
```

Each node writes its runtime log to `dht-test.log` in the working directory.

If you encounter a `Too many open files` error, see [Releasing resource limits](#releasing-resource-limits) below.

## Releasing resource limits

**Note: this section is optional**, but if you run into problems while running the in-process tests, you can try the following configuration.

Raise some of the resource limits this project needs, including the port range, the maximum number of open files, and the TCP MSL.

```bash
sudo vim /etc/sysctl.conf  # append the following lines
net.ipv4.ip_local_port_range = 21000 65535
net.ipv4.tcp_fin_timeout = 4
```

```bash
sudo vim /etc/security/limits.conf  # append the following lines
* soft nofile 65535
* hard nofile 65535
```

```bash
reboot  # restart
```

## References

- [Resource limits on macOS, and the difference between ulimit, launchctl, and sysctl](https://blog.csdn.net/Lockheed_Hong/article/details/75258600)
- [Understanding MSL in TCP/IP](https://blog.51cto.com/u_10706198/1775555)
- [What exactly is GO111MODULE?](https://zhuanlan.zhihu.com/p/374372749)
