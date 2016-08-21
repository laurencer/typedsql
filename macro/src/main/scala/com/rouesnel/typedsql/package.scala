package com.rouesnel

import com.rouesnel.typedsql.core.Partitions
import com.twitter.scrooge.ThriftStruct

package object typedsql {
  type Unpartitioned[T <: ThriftStruct]  = DataSource[T, Partitions.None]
  type Partitioned[T <: ThriftStruct, P] = DataSource[T, P]
}
