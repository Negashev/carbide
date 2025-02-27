# carbide

registry is `bbaue9sa2q4dsifllkti.containers.yandexcloud.net`

usage:
```bash
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products k3s=v1.32.1+k3s1
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products rke2=v1.31.3+rke2r1
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products rancher=2.10.2
```

#### Add custom helm charts with getting images (and oci)

my repo helm repo `http://releases.rancher.com/server-charts/latest` with chart `rancher` and version `2.10.2`

1. remove `http://`

2. replace all `/` in url to `--`

and got `releases.rancher.com--server-charts--latest`

3. to get project, use chart name as product? and add prefix `chart--` with repo
```
rancher=chart--releases.rancher.com--server-charts--latest
```
4. in end add `--` with version of chart
```
rancher=chart--releases.rancher.com--server-charts--latest--2.10.1
```

examples
```
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products rancher=chart--releases.rancher.com--server-charts--latest--2.10.1
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products longhorn=chart--charts.longhorn.io--1.8.0
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products cert-manager=chart--charts.jetstack.io--v1.16.3
hauler store sync -c bbaue9sa2q4dsifllkti.containers.yandexcloud.net --products hauler-helm=oci--ghcr.io--hauler-dev--1.2.1
```