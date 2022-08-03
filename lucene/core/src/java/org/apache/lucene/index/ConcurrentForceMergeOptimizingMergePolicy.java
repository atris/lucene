package org.apache.lucene.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.index.LogDocMergePolicy.DEFAULT_MIN_MERGE_DOCS;


public class ConcurrentForceMergeOptimizingMergePolicy extends LogMergePolicy {

  private static final int DEFAULT_MAX_CONCURRENCY = 5;

  private int maxConcurrency = DEFAULT_MAX_CONCURRENCY;
  private long minNumberOfDocumentsPerMerge = DEFAULT_MIN_MERGE_DOCS;

  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos, int maxNumSegments,
      Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext)
      throws IOException {

    if (maxConcurrency == 1) {
      return super.findForcedMerges(infos, maxNumSegments, segmentsToMerge, mergeContext);
    }

    if (shouldProcess(infos, maxNumSegments, segmentsToMerge, mergeContext) == 0) {
      return null;
    }

    return getForcedMergesInternal(infos.asList(), mergeContext);
  }

  private MergeSpecification getForcedMergesInternal(List<SegmentCommitInfo> infos, MergeContext mergeContext)
      throws IOException {
    MergeSpecification spec = new MergeSpecification();
    List<List<SegmentCommitInfo>> partitionedSegments =
        SegmentListPartitioner.getPartitions(infos, this, mergeContext, maxConcurrency);

    List<OneMerge> merges = new ArrayList<>();
    int i = 0;
    for (List<SegmentCommitInfo> currentPartition : partitionedSegments) {
      OneMerge currentMerge = new OneMerge(currentPartition);

      if (currentMerge.totalMaxDoc < minNumberOfDocumentsPerMerge) {
        if (!merges.isEmpty()) {
          OneMerge previousMerge = merges.get(i);
          List<SegmentCommitInfo> previousSegmentInfos = previousMerge.segments;
          List<SegmentCommitInfo> mergedList = new ArrayList<>(previousSegmentInfos);
          mergedList.addAll(currentPartition);

          merges.remove(i);
          merges.add(new OneMerge(mergedList));
        } else {
          merges.add(new OneMerge(currentPartition));
        }
      } else {
        spec.add(new OneMerge(currentPartition));
      }
    }

    for (OneMerge merge : merges) {
      spec.add(merge);
    }

    //TODO: atri
    System.out.println("CONCUME " + spec.merges.size());
    return spec;
  }

  @Override
  protected long size(SegmentCommitInfo info, MergeContext mergeContext)
      throws IOException {
    return sizeBytes(info, mergeContext);
  }

  public void setMaxConcurrency(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
  }

  public int getMaxConcurrency() {
    return maxConcurrency;
  }

  public void setMinNumberOfDocumentsPerMerge(long minNumberOfDocumentsPerMerge) {
    this.minNumberOfDocumentsPerMerge = minNumberOfDocumentsPerMerge;
  }

  public long getMinNumberOfDocumentsPerMerge() {
    return minNumberOfDocumentsPerMerge;
  }
}
