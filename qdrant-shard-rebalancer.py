#!/usr/bin/env python3

import argparse
import requests

def get_data_from_api(base_url, endpoint):
    response = requests.get(base_url + endpoint)
    response.raise_for_status()
    return response.json()

def parse_cluster_peers(cluster_data):
    peers = cluster_data.get("result", {}).get("peers", {})
    ip_peer_map = {}
    for peer_id, peer_info in peers.items():
        uri = peer_info.get("uri", "")
        ip_address = uri.split("//")[-1].split(":")[0]
        ip_peer_map[ip_address] = int(peer_id)
    return ip_peer_map

def parse_shards(collections_data):
    local_shards = collections_data.get("result", {}).get("local_shards", [])
    remote_shards = collections_data.get("result", {}).get("remote_shards", [])

    peer_shard_map = {}

    for shard in local_shards:
        peer_id = collections_data.get("result", {}).get("peer_id")
        shard_id = shard.get("shard_id")
        peer_shard_map.setdefault(peer_id, []).append(shard_id)

    for shard in remote_shards:
        peer_id = shard.get("peer_id")
        shard_id = shard.get("shard_id")
        peer_shard_map.setdefault(peer_id, []).append(shard_id)

    return peer_shard_map

def rebalance_shards(base_url, collection, peer_shard_map, dry_run=False):
    all_shard_counts = [len(shards) for shards in peer_shard_map.values()]
    if not all_shard_counts:
        print("No shards found to rebalance.")
        return

    average_shards = sum(all_shard_counts) / len(all_shard_counts)
    average_shards_per_peer = int(average_shards)

    underfilled_peers = [peer_id for peer_id, shards in peer_shard_map.items() if len(shards) < average_shards_per_peer]
    overfilled_peers = [peer_id for peer_id, shards in peer_shard_map.items() if len(shards) > average_shards_per_peer]

    print("Underfilled peers:", underfilled_peers)
    print("Overfilled peers:", overfilled_peers)

    rebalance_operations = []

    for overfilled_peer in overfilled_peers:
        for overfilled_shard in peer_shard_map[overfilled_peer][:]:  # make a copy to safely modify
            for underfilled_peer in underfilled_peers:
                if (len(peer_shard_map[underfilled_peer]) < average_shards_per_peer and
                    overfilled_shard not in peer_shard_map[underfilled_peer] and
                    len(peer_shard_map[overfilled_peer]) > average_shards_per_peer):

                    print(f"Moving shard {overfilled_shard} from {overfilled_peer} to {underfilled_peer}")
                    rebalance_operations.append((overfilled_peer, underfilled_peer, overfilled_shard))

                    peer_shard_map[underfilled_peer].append(overfilled_shard)
                    peer_shard_map[overfilled_peer].remove(overfilled_shard)
                    break  # Move only one shard per loop iteration

    if not rebalance_operations:
        print("No rebalancing needed.")
        return

    if dry_run:
        print("\nDry run mode. No changes sent to the server.")
        return

    for from_peer, to_peer, shard_id in rebalance_operations:
        url = f"{base_url}/collections/{collection}/cluster"
        payload = {
            "move_shard": {
                "shard_id": shard_id,
                "from_peer_id": from_peer,
                "to_peer_id": to_peer
            }
        }
        print(f"POST {url} - payload: {payload}")
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print(f"✅ Successfully moved shard {shard_id}")
        else:
            print(f"❌ Failed to move shard {shard_id}: {response.status_code} {response.text}")

def main():
    parser = argparse.ArgumentParser(description="Inspect and optionally rebalance Qdrant cluster shards.")
    parser.add_argument("ip", help="Cluster node IP (e.g., 10.0.0.6)")
    parser.add_argument("collection", help="Collection name")
    parser.add_argument("--rebalance", action="store_true", help="Automatically rebalance shards")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    args = parser.parse_args()

    base_url = f"http://{args.ip}:6333"
    cluster_endpoint = "/cluster"
    collections_endpoint = f"/collections/{args.collection}/cluster"

    try:
        cluster_data = get_data_from_api(base_url, cluster_endpoint)
        collections_data = get_data_from_api(base_url, collections_endpoint)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return

    ip_peer_map = parse_cluster_peers(cluster_data)
    peer_shard_map = parse_shards(collections_data)

    ip_shard_map = {
        ip: peer_shard_map.get(peer_id, [])
        for ip, peer_id in ip_peer_map.items()
    }

    print("\nShard distribution by IP:")
    for ip, shard_ids in ip_shard_map.items():
        peer_id = ip_peer_map[ip]
        print(f"IP: {ip}, Peer ID: {peer_id}, Shard IDs: {shard_ids}")

    if args.rebalance:
        print("\n--- Starting Rebalancing ---")
        rebalance_shards(base_url, args.collection, peer_shard_map, dry_run=args.dry_run)

if __name__ == "__main__":
    main()
