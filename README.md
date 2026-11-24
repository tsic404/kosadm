# kosadm

KOS 平台项目：管理 CLI — 安装、引导、升级、卸载、救援、状态。

- 设计：`tsic/kos-design` 仓的 `kosadm/`（范围、契约、验收、决策记录）

## 构建与测试

```bash
make build        # go build ./...
make lint         # gofmt 检查 + go vet
make test         # 单元测试与验收用例（自带 envtest 控制平面）
make acceptance   # 只跑 tests/acceptance/ 下的验收用例
```

`test` 与 `acceptance` 会按 `Makefile` 里固定的 envtest 版本下载 kube-apiserver 与 etcd；
对象定义由 kosd 模块提供，kosadm 不保留第二份副本。

## 命令

```bash
kosadm install   --definitions <CRD 目录> --identity-database <控制面数据库> \
                 --control-plane-node <节点> [--kubeconfig <路径>]
kosadm bootstrap --definitions <CRD 目录> --identity-database <控制面数据库> \
                 --control-plane-node <节点> --nodes <数量> \
                 --provision-command <拉起运行内核并写出 kubeconfig 的命令>
```

`install --external` 走外置控制面形态：`--control-plane-kubeconfig` 指向运行控制面的集群，
`--kubeconfig` 指向被管集群；首个 Owner 的口令只经安装输出给出的引导端点领取一次。

`--identity-database` 是控制面实际服务的那个库：安装把首个 Owner 写进去，渲染出的控制面
用同一个绝对路径把它挂进工作负载（`-database`），所以安装输出打印的一次性口令就是 pod
里那个账号。该文件必须在控制面所在节点的文件系统上（与 `/var/lib/kosd/packages` 是同一
种交付模型），工作负载以该文件的属主身份运行；节点是容器时（例如 k3d）要把该目录映射进
节点，否则 pod 看不到安装写下的库。

`--control-plane-node` 是持有该文件的那个节点，安装会把控制面固定到它上面：这个库是
节点本地目录（hostPath），只读到 pod 所在节点的文件系统。控制面一旦落到别的节点，
它会新建空库，账号就登不上。

省略时只在一种情况下推断：在场的节点里恰有一台与执行安装的主机同名（比较口径见
ADM-C-70，去空白、转小写，短名与全限定名只比第一个点之前的部分）。零匹配或多匹配都
不会猜，安装会在写任何对象之前拒绝，并提示用 `--control-plane-node` 指定持有该库的
节点。引导安装按观察到的节点集合判断，多匹配立即拒绝；在 `--node-timeout` 内仍没有
唯一匹配则拒绝继续等待。所以即使只有一个节点，「不填」也不等于「工作负载不固定」：
能唯一匹配时会固定到它，匹配不上时直接失败。

外置形态下这个节点属于运行控制面的那个集群。安装给定时会核对这个节点确实存在，不存在
则在写任何对象之前拒绝；引导安装会在声明的节点等待时限内等它加入。升级只有在同时给出
`--identity-database` 时才接受该参数，并核对它等于命令实际运行的节点（见下）。固定按
节点名绑定，不依赖 hostname 标签。路径经过符号链接时按链接的真实目标投递；目标落在只读包
目录里会被拒绝。

纯平台升级不必重传库参数：省略 `--identity-database` 时沿用安装写在控制面 Deployment
上的投递，包括它的库路径与节点固定。只有节点维护升级（`--node`）要求它，且该文件必须是
命令所在节点上的那一份；显式给的节点只用于校验，不会当作文件来源。

```bash
kosadm uninstall [--namespace <名称>] [--mode existing-cluster|bootstrap|external-control-plane] \
                 [--control-plane-kubeconfig <路径>] [--data retain]
kosadm rescue    [--resource <group/version/resource | version/resource>]... [--namespace <名称>]
kosadm status    [--namespace <名称>]
# 纯平台升级：不传库参数，沿用安装写下的投递（库路径与节点固定）
kosadm upgrade platform  --definitions <CRD 目录> --image <控制面镜像> \
                         --installed-framework <版本>... \
                         --package <包引用>=<本地文件>...
# 节点维护升级：摘除节点，维护记录写进命令所在节点上的那份库
kosadm upgrade platform  --definitions <CRD 目录> --image <控制面镜像> \
                         --identity-database <控制面数据库> \
                         [--control-plane-node <节点>] \
                         --node <节点> --version <目标版本> --downtime <预计停机时长>... \
                         --installed-framework <版本>... \
                         --package <包引用>=<本地文件>...
kosadm upgrade extension --package <包引用>=<本地文件>... --target <组件>=<包引用>...
```

`uninstall` 默认保留数据并输出六类遗留清单，同时移除控制面与节点代理及其身份与权限；
数据删除只能走卷路径（KOSD-C-04），命令不提供一键清空。`rescue` 只读原生资源，不写对象、
不启动调和。`status` 只读地报告安装命名空间、控制面副本、对象定义与组件就绪状态。

`upgrade platform` 先替换对象定义、再替换控制面；给出 `--node` 时按节点维护流程预检、
摘除、停止 node-local 应用并驱逐其余工作负载，节点报告 `--version` 后恢复。
预检有不可判定项时整次升级不发起。

节点维护升级（带 `--node`）要给出 `--identity-database`：摘除与恢复的记录写进该库，升级
期间节点设置因此显示「计划维护」，恢复后该归属清除。该文件指的是命令所在节点上的那一份，
必须是已存在的普通文件（升级不建库，路径不存在即拒绝）；`--control-plane-node` 只用于核对，
给定时必须等于命令实际运行的节点。纯平台升级（不带 `--node`）不需要它：不传时沿用安装写在
控制面工作负载上的投递，把操作者引向本机同名副本反而会改动另一份库。

`--installed-framework` 与每个已安装组件对应的 `--package` 都是必需输入：
替换对象定义前先判断这次发行是否移除仍有扩展依赖的框架 API 版本，仍有依赖则整次升级不发起
并列出依赖者；漏给某个已安装组件的包时同样不发起，而不是把该扩展当作不依赖。

`upgrade extension` 以替换组件声明发起：先给出升级前后的贡献差异与检查问题，
检查失败时拒绝升级并列出原因，通过后写入替换声明并跑一次安装流程。

节点维护是升级的内部步骤，不另设子命令。
