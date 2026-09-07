"""Build an allowlisted colleague bundle from an already-tested wheel."""
import argparse
from email.parser import BytesParser
import hashlib
from pathlib import Path
import re
import zipfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('wheel', type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    with zipfile.ZipFile(args.wheel) as wheel:
        metadata_paths = [n for n in wheel.namelist() if n.endswith('.dist-info/METADATA')]
        if len(metadata_paths) != 1:
            raise ValueError('Expected one package in the wheel')
        metadata = BytesParser().parsebytes(wheel.read(metadata_paths[0]))
        version = metadata['Version']
        if metadata['Name'] != 'local-peer-chat' or not re.fullmatch(r'\d+\.\d+\.\d+', version):
            raise ValueError('Expected a released local-peer-chat wheel')
    files = {
        args.wheel.name: args.wheel.read_bytes(),
        'install.sh': (root / 'install.sh').read_bytes(),
        'README.md': (root / 'sharing/README.md').read_text().replace('peer-chat-0.5.4', f'peer-chat-{version}').encode(),
        'REFERENCE.md': (root / 'README.md').read_bytes(),
        'docs/VALIDATION.md': (root / 'docs/VALIDATION.md').read_bytes(),
    }
    files['SHA256SUMS'] = ''.join(f'{hashlib.sha256(data).hexdigest()}  {name}\n'
                                for name, data in files.items()).encode()
    target = root / 'dist' / f'peer-chat-{version}-share.zip'
    target.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(target, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, data in files.items():
            entry = zipfile.ZipInfo(f'peer-chat-{version}/{name}')
            entry.external_attr = (0o100755 if name == 'install.sh' else 0o100644) << 16
            entry.compress_type = zipfile.ZIP_DEFLATED
            bundle.writestr(entry, data)
    print(target)


if __name__ == '__main__':
    main()
