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
              snapper_id = int(os.path.basename(os.path.dirname(local_subv.path)))
              self.add(subv_t(snapper_id, path, parts[10]))

    def get_mismatches(self, targets):
        r =[]
        for path, subv in targets.paths.items():
            ref = self.paths.get(path)
            if ref and (ref.id != subv.id or ref.uuid != subv.uuid):
                print(f"No match for {subv}")
                r+=[ref]
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


class remote_btrfs_target_t:
    def __init__(self, user, host, mnt):
        self.user = user
        self.host = host
        self.mnt = mnt

    def get_subv_map(self):
        cmd = f"ssh {self.user}@{self.host} 'btrfs subv list -o -p -u \"{self.mnt}\"/'"
        print("CMD:",cmd)
        with os.popen(cmd) as p:
            lines = p.read().split('\n')
        r = subv_map_t()
        r.from_subv_list(lines)
        return r

    def delete_subvs(self, doomed_list):
        for subv in doomed_list:
            parent_path = os.path.dirname(subv.path)
            cmd = f"ssh {self.host} 'btrfs subv del {self.mnt}/{subv.path} && rm -rf {self.mnt}/{parent_path}'"
            print("CMD:", cmd)
            if not dryrun:
                self.client.exec_command(cmd)

    def push_local(self, local_mnt, local_parent_ref, local_subv):
        parent_path = os.path.dirname(local_subv.path)
        cmd = f'btrfs send -p "{local_mnt}"/"{local_parent_ref.path}" "{local_mnt}"/"{local_subv.path}" | '
        cmd += f"ssh {self.user}@{self.host} 'mkdir -p \"{self.mnt}\"/\"{parent_path}\" && btrfs receive \"{self.mnt}\"/\"{parent_path}\"/'"
        print("CMD:", cmd)
        if not dryrun:
            err = os.system(cmd)
            if err:
                exit(err)
        cmd = f'scp "{local_mnt}"/"{parent_path}"/info.xml {self.user}@{self.host}:"{self.mnt}"/"{parent_path}"/'
        print("CMD:", cmd)
        if not dryrun:
            err = os.system(cmd)
            if err:
                exit(err)



def get_local_subv_map(mnt):
    cmd = f'btrfs subv list -o -p -u "{mnt}"/'
    print("CMD:", cmd)
    lines=os.popen(cmd).read().split('\n')
    r = subv_map_t()
    r.from_subv_list(lines)
    return r


parser = argparse.ArgumentParser("simple_example")
parser.add_argument("--src", help="Source path of Snapper snapshots.", type=str)
parser.add_argument("--dst", help="Destination path of Snapper snapshots.", type=str)
parser.add_argument("--host", help="Host of desintation.", type=str)
parser.add_argument("--user", help="Username for desintation.", type=str)
parser.add_argument('--dryrun', help="Don't acturally do it.", action='store_true')


if __name__ == '__main__':
    args = parser.parse_args()
    dryrun = args.dryrun
    remote = remote_btrfs_target_t(args.user, args.host, args.dst)

    refs = get_local_subv_map(args.src)
    targets = remote.get_subv_map()

    mismatches = refs.get_mismatches(targets)
    remote.delete_subvs(mismatches)

    matches = refs.get_matches(targets)

    if not matches:
        print("No matches found", refs.paths.keys(), targets.paths.keys())
        exit(-1)

    ids = [ subv.id for subv in matches ]
    ids.sort()

    highest_match_id = ids[-1]

    new_refs = list(filter(lambda ref : True if ref.id > highest_match_id else False, refs.ids.values()))
    parent_ref = refs.ids[highest_match_id]

    for ref in new_refs:
        remote.push_local(args.src, parent_ref, ref)


