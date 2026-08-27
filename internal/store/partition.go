package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/couchgres/couchgres/internal/couch"
)

// PartitionStats is the exact logical metadata behind GET /_partition/{name}.
// ExternalSize counts the winning live bodies and their decoded attachments.
type PartitionStats struct {
	DocCount     int64
	DocDelCount  int64
	ExternalSize int64
}

// GetPartitionStats reads at most sixteen stripe rows instead of scanning the
// partition's documents, revision bodies, and attachments.
func (s *Store) GetPartitionStats(ctx context.Context, db *DB, partition string) (PartitionStats, error) {
	var stats PartitionStats
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT coalesce(sum(doc_count), 0),
		        coalesce(sum(doc_del_count), 0),
		        coalesce(sum(external_size), 0)
		 FROM %s.partition_stats WHERE partition = $1`, db.Schema), partition,
	).Scan(&stats.DocCount, &stats.DocDelCount, &stats.ExternalSize)
	return stats, err
}

// partitionForWrite returns the partition key for a user document. Design and
// local documents are outside partition accounting.
func partitionForWrite(db *DB, id string) (string, bool) {
	if !db.Partitioned {
		return "", false
	}
	partition := PartitionOf(id)
	return partition, partition != ""
}

// lockPartitionWrites serializes growth decisions across processes. The SQL
// function sorts and deduplicates keys before taking transaction-level locks,
// so a batch touching multiple partitions cannot invert their lock order.
func lockPartitionWrites(ctx context.Context, tx pgx.Tx, db *DB, partitions []string) error {
	if len(partitions) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx,
		"SELECT couchgres.lock_partition_writes($1, $2)", db.Schema, partitions)
	return err
}

func partitionSizesTx(
	ctx context.Context,
	tx pgx.Tx,
	db *DB,
	partitions []string,
) (map[string]int64, error) {
	sizes := make(map[string]int64, len(partitions))
	for _, partition := range partitions {
		sizes[partition] = 0
	}
	if len(partitions) == 0 {
		return sizes, nil
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(
		`SELECT partition, coalesce(sum(external_size), 0)
		 FROM %s.partition_stats
		 WHERE partition = ANY($1)
		 GROUP BY partition`, db.Schema), partitions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var partition string
		var size int64
		if err := rows.Scan(&partition, &size); err != nil {
			return nil, err
		}
		sizes[partition] = size
	}
	return sizes, rows.Err()
}

func partitionOverflow(id string) error {
	return couch.NewError(403, "partition_overflow",
		"Partition limit exceeded due to update on '"+id+"'")
}
