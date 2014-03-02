Why the 2nd version?

Mainly to address the followings:
1. Minimize dependencies from Journals to Segments
2. LRU/LRW and more eviction strategies support with efficiency
3. Better boundary protection over max out segments, journals
    * Journals must be rotated
    * Rotation must be logged
    * Manifest must be flushed at each segment splits
    * Cold cache loading should honor the rotation logged
    * Cold cache loading should honor max numbers changes if any
4. Better use of generics, eliminate wherever unneeded
