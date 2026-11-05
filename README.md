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
kosadm install   --definitions <CRD 目录> --identity-database <控制面数据库> [--kubeconfig <路径>]
kosadm bootstrap --definitions <CRD 目录> --identity-database <控制面数据库> --nodes <数量> \
                 --provision-command <拉起运行内核并写出 kubeconfig 的命令>
```

`install --external` 走外置控制面形态：`--control-plane-kubeconfig` 指向运行控制面的集群，
`--kubeconfig` 指向被管集群；首个 Owner 的口令只经安装输出给出的引导端点领取一次。

```bash
kosadm upgrade platform  --definitions <CRD 目录> --image <控制面镜像> \
                         --identity-database <控制面数据库> \
                         [--node <节点> --version <目标版本> --downtime <预计停机时长>]... \
                         --installed-framework <版本>... \
                         --package <包引用>=<本地文件>...
kosadm upgrade extension --package <包引用>=<本地文件>... --target <组件>=<包引用>...
```

`upgrade platform` 先替换对象定义、再替换控制面；给出 `--node` 时按节点维护流程预检、
摘除、停止 node-local 应用并驱逐其余工作负载，节点报告 `--version` 后恢复。
预检有不可判定项时整次升级不发起。

摘除与恢复的记录写进 `--identity-database` 指向的控制面数据库，升级期间节点设置因此显示
「计划维护」，恢复后该归属清除。该数据库必须是已存在的普通文件：升级不建库，路径不存在
即拒绝。`--installed-framework` 与每个已安装组件对应的 `--package` 都是必需输入：
替换对象定义前先判断这次发行是否移除仍有扩展依赖的框架 API 版本，仍有依赖则整次升级不发起
并列出依赖者；漏给某个已安装组件的包时同样不发起，而不是把该扩展当作不依赖。

`upgrade extension` 以替换组件声明发起：先给出升级前后的贡献差异与检查问题，
检查失败时拒绝升级并列出原因，通过后写入替换声明并跑一次安装流程。

节点维护是升级的内部步骤，不另设子命令。
