
# PageRank

This MapReduce program uses hadoop framework. It iteratively runs Mapper and Reducer classes until each node converges and the difference between old Rank and new Rank is lessthan 0.001
This also find Graph properties (Number of nodes, max degree, min degree, avg degree). And also the top 10 pages with high page ranks.

I have assumed Damping Factor as 0.85.
