package bboltstore

import (
	"encoding/binary"
	"fmt"

	"github.com/opentalon/tln-db/proto/tlndbpb"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// Replication op-log layout (global, not per-tenant):
//
//	oplog       bucket: key = 8-byte big-endian uint64 seq
//	                    value = proto-marshaled tlndbpb.OpLogEntry
//	oplog_meta  bucket: "next_seq" -> next uint64 to assign
//	                    "min_seq"  -> earliest retained seq (0 = none yet)
//	repl_meta   bucket: "applied_seq" -> last seq a follower has applied
//
// Entries are appended inside the same bbolt write transaction as the
// mutation they describe, so the log is atomic with the data, gap-free,
// and ordered by commit (bbolt serializes writers).
const (
	oplogBucket     = "oplog"
	oplogMetaBucket = "oplog_meta"
	replMetaBucket  = "repl_meta"

	nextSeqKey    = "next_seq"
	minSeqKey     = "min_seq"
	appliedSeqKey = "applied_seq"
)

func seqKey(seq uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	return b[:]
}

func readMetaUint64(tx *bolt.Tx, bucket, key string) uint64 {
	b := tx.Bucket([]byte(bucket))
	if b == nil {
		return 0
	}
	v := b.Get([]byte(key))
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

func writeMetaUint64(tx *bolt.Tx, bucket, key string, v uint64) error {
	b, err := tx.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return err
	}
	return b.Put([]byte(key), seqKey(v))
}

// appendOpLog assigns the next seq to entry, writes it, and advances
// oplog_meta. Used on the leader write path. Returns the assigned seq.
func appendOpLog(tx *bolt.Tx, entry *tlndbpb.OpLogEntry) (uint64, error) {
	next := readMetaUint64(tx, oplogMetaBucket, nextSeqKey)
	if next == 0 {
		next = 1
	}
	entry.Seq = next
	if err := putOpLogEntry(tx, entry); err != nil {
		return 0, err
	}
	if err := writeMetaUint64(tx, oplogMetaBucket, nextSeqKey, next+1); err != nil {
		return 0, err
	}
	if readMetaUint64(tx, oplogMetaBucket, minSeqKey) == 0 {
		if err := writeMetaUint64(tx, oplogMetaBucket, minSeqKey, next); err != nil {
			return 0, err
		}
	}
	return next, nil
}

// putOpLogEntryAt writes an entry verbatim at entry.Seq (follower apply)
// and forces next_seq/min_seq to stay consistent with the leader.
func putOpLogEntryAt(tx *bolt.Tx, entry *tlndbpb.OpLogEntry) error {
	if err := putOpLogEntry(tx, entry); err != nil {
		return err
	}
	if err := writeMetaUint64(tx, oplogMetaBucket, nextSeqKey, entry.Seq+1); err != nil {
		return err
	}
	if readMetaUint64(tx, oplogMetaBucket, minSeqKey) == 0 {
		if err := writeMetaUint64(tx, oplogMetaBucket, minSeqKey, entry.Seq); err != nil {
			return err
		}
	}
	return nil
}

func putOpLogEntry(tx *bolt.Tx, entry *tlndbpb.OpLogEntry) error {
	b, err := tx.CreateBucketIfNotExists([]byte(oplogBucket))
	if err != nil {
		return err
	}
	raw, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("bboltstore: marshal oplog entry: %w", err)
	}
	return b.Put(seqKey(entry.Seq), raw)
}

// readOpLogFrom iterates entries with seq >= fromSeq in order, invoking
// fn for each. It returns the seq of the last entry passed to fn (0 if
// none). Runs inside a read tx supplied by the caller.
func readOpLogFrom(tx *bolt.Tx, fromSeq uint64, fn func(*tlndbpb.OpLogEntry) error) (uint64, error) {
	b := tx.Bucket([]byte(oplogBucket))
	if b == nil {
		return 0, nil
	}
	c := b.Cursor()
	var last uint64
	for k, v := c.Seek(seqKey(fromSeq)); k != nil; k, v = c.Next() {
		entry := &tlndbpb.OpLogEntry{}
		if err := proto.Unmarshal(v, entry); err != nil {
			return last, fmt.Errorf("bboltstore: unmarshal oplog entry: %w", err)
		}
		if err := fn(entry); err != nil {
			return last, err
		}
		last = entry.Seq
	}
	return last, nil
}

// trimOpLog deletes entries older than the retention window, keeping at
// most `retention` most-recent entries, and advances min_seq. A no-op
// when retention == 0 (keep everything).
func (s *Store) trimOpLog(retention uint64) error {
	if retention == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		next := readMetaUint64(tx, oplogMetaBucket, nextSeqKey)
		if next <= retention+1 {
			return nil // nothing old enough to trim
		}
		floor := next - 1 - retention // delete seq <= floor
		b := tx.Bucket([]byte(oplogBucket))
		if b == nil {
			return nil
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if binary.BigEndian.Uint64(k) > floor {
				break
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return writeMetaUint64(tx, oplogMetaBucket, minSeqKey, floor+1)
	})
}
