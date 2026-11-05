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
