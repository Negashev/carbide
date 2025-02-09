import io
import os
import json
from itertools import chain

import yaml
from hashlib import sha256
import logging

from pyhelm3 import Client as Helm3Client
from minio import Minio
import uvicorn
import aiohttp
from fastapi import FastAPI, HTTPException
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

OSClient = Minio(os.getenv("MINIO_ENDPOINT", "storage.yandexcloud.net"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=os.getenv("MINIO_SECURE", True),
)

helmClient = Helm3Client()

OSBucketName = os.getenv("MINIO_BUCKET_NAME", "carbide")

# Make the bucket if it doesn't exist.
def check_bucket(bucket):
    found = OSClient.bucket_exists(bucket)
    if not found:
        logging.error(f"Created bucket {bucket}")
    else:
        logging.info(f"Bucket {bucket}already exists")

def set_object(key, value):
    return OSClient.put_object(
            bucket_name=OSBucketName,
            object_name=key,
            data=io.BytesIO(value),
            length=len(value),
    )

def get_object(key):
    return OSClient.get_object(
            bucket_name=OSBucketName,
            object_name=key
    )

app = FastAPI()


def find_images(data, results=None):
    if results is None:
        results = []

    # Если data — словарь, проверяем его ключи и значения
    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'image' and type(value) == str:
                results.append(value)
            # Рекурсивно проверяем значение, если оно словарь или список
            find_images(value, results)

    # Если data — список, проверяем каждый элемент
    elif isinstance(data, list):
        for item in data:
            find_images(item, results)

    return results

async def download_file(url: str) -> bytes:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                raise HTTPException(status_code=404, detail=f"File not found: {url}")
            return await response.read()

async def generate_json(name, kind, data, tag, parse_tag=True):
    spec_type = kind.lower()
    spec_name = name
    spec_data = []
    if parse_tag:
        tag = tag.replace("-", "+")
    if spec_type == "charts":
        for item in data:
            # replace {version} if exist
            item["version"] = item["version"].format(version=tag)
            spec_data.append(item)
    if spec_type == "files":
        for item in data:
            replace_item = item
            replace_item['path'] = item['path'].format(version=tag)
            spec_data.append(replace_item)
    if spec_type == "images-list":
        kind = "Images"
        spec_type = "images"
        for item in data:
            url = item["url"].format(version=tag)
            text = await download_file(url)
            images = text.decode("utf-8").split('\n')
            for image in images:
                if image != '':
                    spec_data.append({"name": image, "platform": item["platform"]})
            spec_name = spec_name + "-" + item["platform"]
    if spec_type == "charts-images":
        kind = "Images"
        spec_type = "images"
        for item in data:
            chart = await helmClient.get_chart(
                item["name"],
                repo=item["repoURL"],
                version=item["version"].format(version=tag.replace("-", "+"))
            )
            template = await helmClient.template_resources(chart, release_name=item["name"], include_crds=True, no_hooks=False)
            for file in template:
                images = find_images(file)
                for image in images:
                    spec_data.append({"name": image})

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

def parse_helm_url(helm_url_with_version):
        repo = helm_url_with_version[len("chart--"):]
        # get last element of url
        repoUrlSplit = repo.split("--")
        version = repoUrlSplit[-1]
        helmRepoUrl = "/".join(repoUrlSplit[0:-1])
        return helmRepoUrl, version


@app.get("/")
@app.get("/v2/")
async def root():
    return Response(status_code=200)


@app.get("/v2/hauler/{repo}-manifest.yaml/manifests/{tag}")
async def get_manifest(repo: str, tag: str):
    # check if project exist
    json_array = []
    if tag.startswith("sha256"):
        raise  HTTPException(status_code=404, detail="Manifest not found")
    elif tag.startswith("chart--"):
        helmRepoUrl, version = parse_helm_url(tag)
        json_data = await generate_json(repo, "Charts",
            [{
                "repoURL": "http://" + helmRepoUrl,
                "name": repo,
                "version": version
            }], version, False)
        json_array.append(json_data)
        json_data = await generate_json(repo, "Charts-images",
            [{
                "repoURL": "http://" + helmRepoUrl,
                "name": repo,
                "version": version
            }], version, False)
        json_array.append(json_data)
    else:
        if repo in PROJECTS.keys():
            # get all kind
            for kind in PROJECTS[repo].keys():
                json_data = await generate_json(repo, kind, PROJECTS[repo][kind], tag)
                json_array.append(json_data)
    if not json_array:
        raise HTTPException(status_code=404, detail="Manifest not found")
    body = get_hauler(json_array)
    blob_id = "sha256:" + sha256(body.encode('utf-8')).hexdigest()
    set_object(repo + "_" + blob_id, body.encode('utf-8'))
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
async def get_blob(repo: str, blob_id: str):
    body = get_object(repo + "_" + blob_id)
    return Response(
        content=body.read(),
        media_type = "application/x-yaml"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    check_bucket(OSBucketName)
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))