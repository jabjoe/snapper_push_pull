#! /usr/bin/python3

import os
import shlex
import argparse
import logging
import string
from pathlib import Path
from collections import namedtuple

parser = argparse.ArgumentParser("Snapper_push_pull")
parser.add_argument('src', metavar='src_path', type=str, nargs='?',
                    help='snapper controlled source directory: /mnt/mylocal/@snaps or root@remote:/mnt/backup/@snaps')
parser.add_argument('dst', metavar='dst_path', type=str, nargs='?',
                    help='destination directory: /mnt/mylocal/@snaps or root@remote:/mnt/backup/@snaps')
parser.add_argument(
    '--dryrun', help="Don't acturally do it.", action='store_true')
parser.add_argument('-v', '--verbose', help="Info log.", action='store_true')
parser.add_argument('-d', '--debug', help="Debug log.", action='store_true')
parser.add_argument('-f', '--force', help="Delete subvols at destination that conflict.", action='store_true')
parser.add_argument('-l','--list', help="Just list.", action='store_true')


logger = logging.getLogger("snapper_push_push")

subv_t = namedtuple('subv_t', ['id', 'path', 'uuid'])

dryrun = False

force = False


class subv_map_t:
    def __init__(self):
        self.ids = {}
        self.paths = {}
        self.uuids = {}

    def add(self, subv):
        self.ids[subv.id] = subv
        self.paths[subv.path] = subv
        self.uuids[subv.uuid] = subv

    def remove(self, subv):
        self.ids.pop(subv.id, None)
        self.paths.pop(subv.path, None)
        self.uuids.pop(subv.uuid, None)

    def from_subv_list(self, lines):
        for line in lines:
            parts = shlex.split(line)
            subv = None
            if parts:
                part_parts = Path(parts[12]).parts
                uuid = parts[10]
                if uuid == '-':
                    continue
                uuid_parts = uuid.split('-')
                assert len(uuid) == 36 and \
                    len(uuid_parts) == 5 and \
                    min(map(lambda s: min([c in string.hexdigits for c in s]), [p for p in uuid_parts])), \
                    "Columnn 10 was not a Btrfs UUID"
                if part_parts[-1] == "snapshot":
                    try:
                        snapper_id = int(part_parts[-2])
                    except ValueError:
                        snapper_id = None
                    if snapper_id:
                        subv = subv_t(snapper_id,
                                        f"{snapper_id}/snapshot",
                                        uuid)
                        self.add(subv)
            if subv:
                logger.debug(f"Found snapper ID {subv.id} for line: {line}")
            else:
                logger.debug(f"No snapper ID found for line: {line}")

    def get_mismatches(self, targets):
        r = []
        for path, subv in targets.paths.items():
            ref = self.paths.get(path)
            if ref:
                if ref.id != subv.id or ref.uuid != subv.uuid:
                    logger.error(f"Bad match {subv} {ref}")
                    if force:
                        r += [subv]
                    else:
                        exit(-1)
            else:
                logger.info(f"No match for {subv}")
                r += [subv]
        for subv in r:
            targets.remove(subv)
        return r

    def get_matches(self, targets):
        r = []
        for path, subv in targets.paths.items():
            ref = self.paths.get(path)
            if ref and ref.uuid == subv.uuid and ref.id == subv.id:
                r += [ref]
        return r


class local_btrfs_t:
    def __init__(self, mnt):
        self.mnt = mnt

    def __str__(self):
        return f"{self.mnt}"

    def get_subv_recv_list_cmd(self):
        return f'btrfs subv list -o -p -R "{self.mnt}"/'

    def get_subv_send_list_cmd(self):
        return f'btrfs subv list -o -p -u "{self.mnt}"/'

    def get_del_cmd(self, subv):
        parent_path = os.path.dirname(subv.path)
        return f'[ -e "{self.mnt}"/"{subv.path}" ] && btrfs subv del "{self.mnt}"/"{subv.path}" && rm -rf "{self.mnt}"/"{parent_path}" || exit 0'

    def get_send_cmd(self, parent_subv, subv):
        if parent_subv:
            return f'btrfs send -p "{self.mnt}"/"{parent_subv.path}" "{self.mnt}"/"{subv.path}"'
        else:
            return f'btrfs send "{self.mnt}"/"{subv.path}"'

    def get_pre_recv_cmd(self, parent_path):
        return f'mkdir -p "{self.mnt}"/"{parent_path}"'

    def get_recv_cmd(self, parent_path):
        return f'btrfs receive "{self.mnt}"/"{parent_path}"/'

    def get_info_xml_cmd(self, parent_path):
        return f'cat "{self.mnt}"/"{parent_path}"/info.xml'

    def set_info_xml_cmd(self, parent_path, info_xml):
        return f'cat > "{self.mnt}"/"{parent_path}"/info.xml'

    def _get_subv_map(self, cmd):
        logger.debug(f"CMD: {cmd}")
        lines = os.popen(cmd).read().split('\n')
        r = subv_map_t()
        r.from_subv_list(lines)
        return r

    def get_subv_recv_map(self):
        return self._get_subv_map(self.get_subv_recv_list_cmd())

    def get_subv_send_map(self):
        return self._get_subv_map(self.get_subv_send_list_cmd())

    def delete_subvs(self, doomed_list):
        for subv in doomed_list:
            cmd = self.get_del_cmd(subv)
            logger.debug(f"CMD: {cmd}")
            if not dryrun:
                err = os.system(cmd)
                if err:
                    exit(err)

    def get_info_xml(self, parent_path):
        cmd = self.get_info_xml_cmd(parent_path)
        logger.debug(f"CMD: {cmd}")
        return os.popen(cmd).read()

    def set_info_xml(self, parent_path, info_xml):
        cmd = self.set_info_xml_cmd(parent_path, info_xml)
        logger.debug(f"CMD: {cmd}")
        if not dryrun:
            p = os.popen(cmd, 'w')
            p.write(info_xml)
            p.flush()
            p.close()

    def recv_subvs(self, btrfs_source, parent_subv, subv):
        send_cmd = btrfs_source.get_send_cmd(parent_subv, subv)
        parent_path = os.path.dirname(subv.path)
        pre_recv_cmd = self.get_pre_recv_cmd(parent_path)
        logger.debug(f"CMD: {pre_recv_cmd}")

        if not dryrun:
            err = os.system(pre_recv_cmd)
            if err:
                exit(err)

        recv_cmd = self.get_recv_cmd(parent_path)

        cmd = f"{send_cmd} | {recv_cmd}"
        logger.debug(f"CMD: {cmd}")

        if not dryrun:
            err = os.system(cmd)
            if err:
                exit(err)
            info_xml = btrfs_source.get_info_xml(parent_path)
            self.set_info_xml(parent_path, info_xml)


class remote_btrfs_t(local_btrfs_t):
    def __init__(self, user, host, mnt):
        super().__init__(mnt)
        self.user = user
        self.host = host

    def __str__(self):
        return f"{self.user}@{self.host}:{self.mnt}" if self.user else f"{self.host}:{self.mnt}"

    def _ssh_wrap_cmd(self, cmd):
        return f"ssh {self.user}@{self.host} '{cmd}'" if self.user else f"ssh {self.host} '{cmd}'"

    def get_subv_recv_list_cmd(self):
        return self._ssh_wrap_cmd(super().get_subv_recv_list_cmd())

    def get_subv_send_list_cmd(self):
        return self._ssh_wrap_cmd(super().get_subv_send_list_cmd())

    def get_del_cmd(self, subv):
        return self._ssh_wrap_cmd(super().get_del_cmd(subv))

    def get_send_cmd(self, parent_subv, subv):
        return self._ssh_wrap_cmd(super().get_send_cmd(parent_subv, subv))

    def get_pre_recv_cmd(self, parent_path):
        return self._ssh_wrap_cmd(super().get_pre_recv_cmd(parent_path))

    def get_recv_cmd(self, parent_path):
        return self._ssh_wrap_cmd(super().get_recv_cmd(parent_path))

    def get_info_xml_cmd(self, parent_path):
        return self._ssh_wrap_cmd(super().get_info_xml_cmd(parent_path))

    def set_info_xml_cmd(self, parent_path, info_xml):
        return self._ssh_wrap_cmd(super().set_info_xml_cmd(parent_path, info_xml))


def get_btrfs(path):
    if path[0] != '/':
        parts = path.split(':')
        if len(parts) > 1:
            hostname = parts[0]
            if hostname.count("/") == 0:
                mnt = parts[1]
                parts = hostname.split('@')
                if len(parts) > 1:
                    username = parts[0]
                    hostname = parts[1]
                    return remote_btrfs_t(username, hostname, mnt)
                else:
                    return remote_btrfs_t(None, hostname, mnt)
            else:
                logger.error("Invalid path given, not clearly remote or local.")
                exit(-1)
    return local_btrfs_t(path)


if __name__ == '__main__':
    args = parser.parse_args()
    dryrun = args.dryrun

    if not args.src or not args.dst:
        parser.print_help()
        exit(-1)

    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    if args.force:
        force = True

    src = get_btrfs(args.src)
    dst = get_btrfs(args.dst)

    logger.info(f'"{src}" -> "{dst}"')

    src_subvs = src.get_subv_send_map()
    if not src_subvs:
        logger.error("No source snapshots.")
        exit(-1)
    dst_subvs = dst.get_subv_recv_map()

    if args.list:
        print("="*72)
        print("Source:")
        print("="*72)
        for sub_v in src_subvs.ids.values():
            print(f"src subv: id:{sub_v.id} uuid:{sub_v.uuid} path:{sub_v.path}")

        print("="*72)
        print("Destination")
        print("="*72)
        for sub_v in dst_subvs.ids.values():
            print(f"dst subv: id:{sub_v.id} uuid:{sub_v.uuid} path:{sub_v.path}")
        exit(0)

    mismatches = src_subvs.get_mismatches(dst_subvs)
    dst.delete_subvs(mismatches)

    if len(dst_subvs.paths):
        matches = src_subvs.get_matches(dst_subvs)

        if not matches:
            logger.error("No matches found")
            exit(-1)

        ids = [subv.id for subv in matches]
        ids.sort()

        highest_match_id = ids[-1]

        new_refs = list(filter(lambda ref: True if ref.id >
                        highest_match_id else False, src_subvs.ids.values()))
        parent_ref = src_subvs.ids[highest_match_id]
    else:
        logger.error("No matching destinations, starting fresh.")

        ids = list(src_subvs.ids.keys())
        ids.sort()

        new_refs = [src_subvs.ids[ref_id] for ref_id in ids]
        parent_ref = None

    for ref in new_refs:
        dst.recv_subvs(src, parent_ref, ref)
        parent_ref = ref
