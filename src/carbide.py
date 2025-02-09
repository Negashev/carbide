import re
import gzip
import tarfile
from io import BytesIO
import json
import yaml
from hashlib import sha256
import logging
import aiohttp
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response

PROJECTS = {
    "k3s": {
        "Files": [
            {"path": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s"},
            {"path": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s-arm64"},
            {"path": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s-armhf"},
            {"path": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s-airgap-images-amd64.tar.zst"},
            {"path": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s-airgap-images-arm.tar.zst"},
            {"path": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s-airgap-images-arm64.tar.zst"}
        ],
        "Images-list": [
            {
                "url": "https://github.com/k3s-io/k3s/releases/download/{version}/k3s-images.txt",
                "platform": "all"
            }
        ]
    },
    "rke2": {
        "Files": [
            {"path": "https://raw.githubusercontent.com/rancher/rke2/refs/tags/{version}/install.sh"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/rke2-images.linux-amd64.tar.zst"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/rke2.linux-amd64.tar.gz"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/sha256sum-amd64.txt"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/rke2.linux-amd64"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/rke2-images.linux-arm64.tar.zst"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/rke2.linux-arm64.tar.gz"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/sha256sum-arm64.txt"},
            {"path": "https://github.com/rancher/rke2/releases/download/{version}/rke2.linux-arm64"}
        ],
        "Images-list": [
            {
                "url": "https://github.com/rancher/rke2/releases/download/{version}/rke2-images-all.linux-arm64.txt",
                "name": "arm64",
                "platform": "linux/arm64"
            },
            {
                "url": "https://github.com/rancher/rke2/releases/download/{version}/rke2-images-all.linux-amd64.txt",
                "name": "amd64",
                "platform": "linux/amd64"
            },
        ]
    },
    "rancher": {
        "Charts": [
            {
                "repoURL": "https://charts.jetstack.io",
                "name": "cert-manager",
                "version": "v1.16.3"
            },
            {
                "repoURL": "https://releases.rancher.com/server-charts/latest",
                "name": "rancher",
                "version": "{version}"
            },
        ],
        "Charts-images": [
            {
                "repoURL": "https://charts.jetstack.io",
                "name": "cert-manager",
                "version": "v1.16.3"
            },
            {
                "repoURL": "https://releases.rancher.com/server-charts/latest",
                "name": "rancher",
                "version": "{version}"
            },
        ],
        "Images-list": [
            {
                "url": "https://github.com/rancher/rancher/releases/download/v{version}/rancher-images.txt",
                "platform": "all"
            }
        ]
    },
    "longhorn": {
        "Charts": [
            {
                "repoURL": "https://charts.longhorn.io",
                "name": "longhorn",
                "version": "{version}"
            },
        ],
        "Images-list": [
            {
                "url": "https://raw.githubusercontent.com/longhorn/longhorn/v{version}/deploy/longhorn-images.txt",
                "platform": "all"
            }
        ]
    }
}


async def on_fetch(request, env):
    import asgi
    logging.basicConfig(level=logging.DEBUG)
    return await asgi.fetch(app, request, env)


app = FastAPI()


async def check_docker_image_exists(image_str: str, username: str = None, password: str = None) -> bool:

    def is_registry(s: str) -> bool:
        return ':' in s or '.' in s or s == 'localhost'

    def parse_image(image_str: str) -> tuple:
        if ':' in image_str:
            image_part, tag = image_str.rsplit(':', 1)
        else:
            image_part, tag = image_str, 'latest'

        parts = image_part.split('/', 1)
        if len(parts) == 1 or not is_registry(parts[0]):
            registry = 'docker.io'
            image_name = image_part
        else:
            registry, image_name = parts

        if registry == 'docker.io' and '/' not in image_name:
            image_name = f'library/{image_name}'

        if registry == 'docker.io':
            registry_url = 'https://registry-1.docker.io'
        elif ':' in registry or registry.startswith('localhost'):
            registry_url = f'http://{registry}' if registry.startswith('localhost') else f'https://{registry}'
        else:
            registry_url = f'https://{registry}'

        return registry_url, image_name, tag

    registry_url, image_name, tag = parse_image(image_str)
    url = f"{registry_url}/v2/{image_name}/manifests/{tag}"
    headers = {'Accept': 'application/vnd.docker.distribution.manifest.v2+json'}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers,
                                   auth=aiohttp.BasicAuth(username, password) if username else None) as response:
                if response.status == 401 and 'Www-Authenticate' in response.headers:
                    auth_header = response.headers['Www-Authenticate']
                    if auth_header.startswith('Bearer '):
                        auth_params = {}
                        for part in auth_header[7:].split(','):
                            key, value = part.strip().split('=', 1)
                            auth_params[key] = value.strip('"')

                        token_url = auth_params['realm']
                        async with session.get(
                                token_url,
                                params={
                                    'service': auth_params.get('service', ''),
                                    'scope': auth_params.get('scope', '')
                                },
                                auth=aiohttp.BasicAuth(username, password) if username else None
                        ) as token_response:
                            token_response.raise_for_status()
                            token = (await token_response.json())['token']

                        headers['Authorization'] = f'Bearer {token}'
                        async with session.get(url, headers=headers) as final_response:
                            return final_response.status == 200

                return response.status == 200

    except (aiohttp.ClientError, ValueError, KeyError):
        return False

def find_image_tag_pairs(content):
    pairs = []
    pattern = re.compile(
        r'(?i)(repository|image|tag|version):[ ?]([^\s]+)',
        flags=re.IGNORECASE
    )
    pairs = []
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        return pairs

    images = []
    tags = []

    # Search all matches
    matches = pattern.findall(text)
    for key, value in matches:
        if key.lower() == 'image' and value[0].isalpha():
            images.append(value)
        if key.lower() == 'repository' and value[0].isalpha():
            images.append(value)
        if key.lower() == 'version':
            tags.append(value)
        elif key.lower() == 'tag':
            tags.append(value)

    return images, tags

def extract_tgz_in_memory(tgz_data: bytes) -> dict:
    compressed_buffer = BytesIO(tgz_data)

    # unpack GZIP and read TAR
    with gzip.open(compressed_buffer, 'rb') as gz_file:
        with tarfile.open(fileobj=gz_file, mode='r:*') as tar:
            file_contents = {}

            for member in tar.getmembers():
                if member.isfile():  # Пропускаем директории и другие объекты
                    file = tar.extractfile(member)
                    if file:
                        file_contents[member.name] = file.read()
            return file_contents

async def download_file(url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise HTTPException(status_code=404, detail=f"File not found: {url}")
            return await response.read()

async def generate_json(name, kind, data, tag):
    spec_type = kind.lower()
    spec_name = name
    spec_data = []
    if spec_type == "charts":
        for item in data:
            # replace {version} if exist
            item["version"] = item["version"].format(version=tag.replace("-", "+"))
            spec_data.append(item)
    if spec_type == "files":
        for item in data:
            replace_item = item
            replace_item['path'] = item['path'].format(version=tag.replace("-", "+"))
            spec_data.append(replace_item)
    if spec_type == "images-list":
        kind = "Images"
        spec_type = "images"
        for item in data:
            url = item["url"].format(version=tag.replace("-", "+"))
            text = await download_file(url)
            images = text.decode("utf-8").split('\n')
            for image in images:
                if image != '':
                    spec_data.append({"name": image, "platform": item["platform"]})
            spec_name = spec_name + "-" + item["platform"]
    if spec_type == "charts-images":
        # TODO create better solution for helm template :)
        kind = "Images"
        spec_type = "images"
        for item in data:
            helm_url = None
            url = item["repoURL"] + "/index.yaml"
            text = await download_file(url)
            entries = text.decode("utf-8")
            index_data = yaml.safe_load(entries)
            chart_name = item["name"]
            # Find chart in entries
            if chart_name not in index_data.get('entries', {}):
                raise ValueError(f"Chart '{chart_name}' not found in index")
            # Get all versions
            versions = index_data['entries'][chart_name]
            # replace {version} if exist
            item["version"] = item["version"].format(version=tag.replace("-", "+"))
            for version in versions:
                if version['version'] == item['version']:
                    for helm_url in version['urls']:
                        if helm_url.endswith(".tgz"):
                            helm_url = item["repoURL"] + "/" + helm_url
                            tgz_data = await download_file(helm_url)
                            extracted = extract_tgz_in_memory(tgz_data)
                            images = []
                            tags = []
                            pairs = []
                            for filename, content in extracted.items():
                                # if filename.endswith("values.yaml"):
                                tmp_images, tmp_tags = find_image_tag_pairs(content)
                                images = images + tmp_images
                                tags = tags + tmp_tags
                            images = list(dict.fromkeys(images))
                            tags = list(dict.fromkeys(tags))
                            # create all images with check
                            for image in images:
                                latest_tags = tags.copy()
                                for tmp_tags in latest_tags:
                                    repo_exits = await check_docker_image_exists(f"{image}:{tmp_tags}")
                                    if repo_exits:
                                        pairs.append(f"{image}:{tmp_tags}")
                                    else:
                                        tags.remove(tmp_tags)
                            pairs = list(dict.fromkeys(pairs))
                            for result in pairs:
                                spec_data.append({"name": result})

    return {
        "apiVersion": "content.hauler.cattle.io/v1alpha1",
        "kind": kind,
        "metadata": {
            "name": f"{spec_name}-airgap-{spec_type}"
        },
        "spec": {
            spec_type: spec_data
        }
    }


def get_hauler(json_array):
    yaml_data = []
    for json_data in json_array:
        yaml_data.append(yaml.dump(json_data, default_flow_style=False))
    data = '---\n'.join(yaml_data)
    return data


@app.get("/")
@app.get("/v2/")
async def root():
    return Response(status_code=200)


@app.get("/v2/hauler/{repo}-manifest.yaml/manifests/{tag}")
async def get_manifest(repo: str, tag: str, req: Request):
    env = req.scope["env"]
    if tag.startswith("sha256"):
        raise HTTPException(status_code=404, detail="Manifest not found")
    # check if project exist
    json_array = []
    if repo in PROJECTS.keys():
        # get all kind
        for kind in PROJECTS[repo].keys():
            json_data = await generate_json(repo, kind, PROJECTS[repo][kind], tag)
            json_array.append(json_data)
    if not json_array:
        raise HTTPException(status_code=404, detail="Manifest not found")
    body = get_hauler(json_array)
    blob_id = "sha256:" + sha256(body.encode('utf-8')).hexdigest()
    await env.CARBIDE.put(repo + "_" + blob_id, body)
    manifest_data = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "artifactType": "application/vnd.unknown.artifact.v1",
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "size": 2,
            "data": "e30="
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar",
                "digest": blob_id,
                "size": len(body),
                "annotations": {
                    "org.opencontainers.image.title": repo + "-manifest.yaml"
                }
            }
        ]
    }
    return Response(
        content=json.dumps(manifest_data),
        media_type="application/vnd.docker.distribution.manifest.v2+json"
    )


@app.get("/v2/hauler/{repo}-manifest.yaml/blobs/{blob_id}")
async def get_blob(repo: str, blob_id: str, req: Request):
    env = req.scope["env"]
    body = await env.CARBIDE.get(repo + "_" + blob_id)
    return Response(
        content=body,
        media_type = "application/x-yaml"
    )
