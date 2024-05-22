#! /usr/bin/python3

import os
import shlex
import argparse
from collections import namedtuple 

subv_t = namedtuple('subv_t', ['id', 'path', 'uuid'])

dryrun = False

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
            if parts:
                path=parts[12]
                try:
                    snapper_id = int(os.path.basename(os.path.dirname(path)))
                except ValueError:
                    snapper_id = None
                if snapper_id:
                    self.add(subv_t(snapper_id, path, parts[10]))

    def get_mismatches(self, targets):
        r =[]
        for path, subv in targets.paths.items():
            ref = self.paths.get(path)
            if ref and (ref.id != subv.id or ref.uuid != subv.uuid):
                print(f"No match for {subv} {ref}")
                r+=[subv]
        for subv in r:
            targets.remove(subv)
        return r
 
    def get_matches(self, targets):
        r = []
        for path, subv in targets.paths.items():
            ref = self.paths.get(path)
            if ref and ref.uuid == subv.uuid and ref.id == subv.id:
                r+=[ref]
        return r


class local_btrfs_t:
    def __init__(self, mnt):
        self.mnt = mnt

    def get_subv_list_cmd(self):
        return f'btrfs subv list -o -p -u "{self.mnt}"/'

    def get_del_cmd(self, subv):
        parent_path = os.path.dirname(subv.path)
        return f'btrfs subv del "{self.mnt}"/"{subv.path}" && rm -rf "{self.mnt}"/"{parent_path}"'

    def get_send_cmd(self, parent_subv, subv):
        return f'btrfs send -p "{self.mnt}"/"{parent_subv.path}" "{self.mnt}"/"{subv.path}"'

    def get_recv_cmd(self, parent_path):
        return f'mkdir -p "{parent_path}" && btrfs receive "{parent_path}"/'

    def get_info_xml_cmd(self, parent_path):
        return f'cat "{self.mnt}"/"{parent_path}"/info.xml'

    def set_info_xml_cmd(self, parent_path, info_xml):
        return f'cat > "{self.mnt}"/"{parent_path}"/info.xml'

    def get_subv_map(self):
        cmd = self.get_subv_list_cmd()
        print("CMD:", cmd)
        lines=os.popen(cmd).read().split('\n')
        r = subv_map_t()
        r.from_subv_list(lines)
        return r

    def delete_subvs(self, doomed_list):
        for subv in doomed_list:
            cmd = self.get_del_cmd(subv)
            print("CMD:", cmd)
            if not dryrun:
                err = os.system(cmd)
                if err:
                    exit(err)

    def get_info_xml(self, parent_path):
        cmd = self.get_info_xml_cmd(parent_path)
        print("CMD:", cmd)
        return os.popen(cmd).read()

    def set_info_xml(self, parent_path, info_xml):
        cmd = self.set_info_xml_cmd(parent_path, info_xml)
        print("CMD:", cmd)
        if not dryrun:
            os.popen(cmd, 'w').write(info_xml)

    def recv_subvs(self, btrfs_source, parent_subv, subv):
        send_cmd = btrfs_source.get_send_cmd(parent_subv, subv)
        parent_path = os.path.dirname(subv.path)
        recv_cmd = self.get_recv_cmd(parent_path)
        cmd = f"{send_cmd} | {recv_cmd}"
        print("CMD:", cmd)
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

    def _ssh_wrap_cmd(self, cmd):
        return f"ssh {self.user}@{self.host} '{cmd}'"

    def get_subv_list_cmd(self):
        return self._ssh_wrap_cmd(super().get_subv_list_cmd())

    def get_del_cmd(self, subv):
        return self._ssh_wrap_cmd(super().get_del_cmd(subv))

    def get_send_cmd(self, parent_subv, subv):
        return self._ssh_wrap_cmd(super().get_send_cmd(parent_subv, subv))

    def get_recv_cmd(self, parent_path):
        return self._ssh_wrap_cmd(super().get_recv_cmd(parent_path))

    def get_info_xml_cmd(self, parent_path):
        return self._ssh_wrap_cmd(super().get_info_xml_cmd(parent_path))

    def set_info_xml_cmd(self, parent_path, info_xml):
        return self._ssh_wrap_cmd(super().set_info_xml_cmd(parent_path, info_xml))



def get_btrfs(path):
    parts = path.split(':')
    if len(parts) > 1:
        hostname = parts[0]
        mnt = parts[1]
        parts = hostname.split('@')
        if len(parts) > 1:
            username = parts[0]
            hostname = parts[1]
            return remote_btrfs_t(username, hostname, mnt)
    return local_btrfs_t(path)


parser = argparse.ArgumentParser("simple_example")
parser.add_argument("--src", help="Source path of Snapper snapshots.", type=str)
parser.add_argument("--dst", help="Destination path of Snapper snapshots.", type=str)
parser.add_argument('--dryrun', help="Don't acturally do it.", action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    dryrun = args.dryrun

    src = get_btrfs(args.src)
    dst = get_btrfs(args.dst)

    src_subvs = src.get_subv_map()
    dst_subvs = dst.get_subv_map()

    mismatches = src_subvs.get_mismatches(dst_subvs)
    dst.delete_subvs(mismatches)

    matches = src_subvs.get_matches(dst_subvs)

    if not matches:
        print("No matches found")
        exit(-1)

    ids = [ subv.id for subv in matches ]
    ids.sort()

    highest_match_id = ids[-1]

    new_refs = list(filter(lambda ref : True if ref.id > highest_match_id else False, src_subvs.ids.values()))
    parent_ref = src_subvs.ids[highest_match_id]

    for ref in new_refs:
        dst.recv_subvs(src, parent_ref, ref)
        parent_ref = ref


