# ITU-BSDISYS1KU-Replication-HandIn5


Design thoughts:

- Use leader-based replication to avoid an overcomplicated system:
  
It's just as good as leader-less for our purpose and is actually better at keeping track of multiple bets submitted at the same time. one leader vs multiple nodes receiving the same bids at different times


- Make a "heartbeat" signal that keeps sending messages between all nodes on a time interval:

This is to ensure they are still alive and to promote a backup to leader in case it fails.

- Promote "algorithm" chooses highest rank node ID:

We give/hardcode each node to have an ID. The leader is chosen based on the lowest ID on the available nodes in the system
