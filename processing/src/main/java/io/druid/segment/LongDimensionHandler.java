/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.selector.settable.SettableColumnValueSelector;
import io.druid.segment.selector.settable.SettableLongColumnValueSelector;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class LongDimensionHandler implements DimensionHandler<Long, Long, Long>
{
  private static Comparator<ColumnValueSelector> LONG_COLUMN_COMPARATOR = (s1, s2) -> {
    if (s1.isNull()) {
      return s2.isNull() ? 0 : -1;
    } else if (s2.isNull()) {
      return 1;
    } else {
      return Long.compare(s1.getLong(), s2.getLong());
    }
  };

  private final String dimensionName;

  public LongDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName;
  }

  @Override
  public DimensionIndexer<Long, Long, Long> makeIndexer()
  {
    return new LongDimensionIndexer();
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    return new LongDimensionMergerV9(
        dimensionName,
        indexSpec,
        segmentWriteOutMedium
    );
  }

  @Override
  public int getLengthOfEncodedKeyComponent(Long dimVals)
  {
    return 1;
  }

  @Override
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return LONG_COLUMN_COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    return new SettableLongColumnValueSelector();
  }
}
