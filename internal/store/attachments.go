package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/klauspost/compress/gzip"

	"github.com/couchgres/couchgres/internal/couch"
)

// Attachment is stored attachment metadata (Data populated on demand).
// CouchDB may store gzip-encoded attachments and replicate them that way.
// Data then holds the encoded bytes, Length the decoded size, and Digest
// covers the encoded form (all matching CouchDB's bookkeeping).
type Attachment struct {
	Name          string
	ContentType   string
	Digest        string // "md5-<base64>"
	RevPos        int
	Length        int64 // decoded length
	Encoding      string
	EncodedLength int64
	Data          []byte
}

// DecodedData returns the attachment content in identity form.
func (a *Attachment) DecodedData() ([]byte, error) {
	if a.Encoding == "" {
		return a.Data, nil
	}
	if a.Encoding != "gzip" {
		return nil, fmt.Errorf("unsupported attachment encoding %q", a.Encoding)
	}
	reader, err := gzip.NewReader(bytes.NewReader(a.Data))
	if err != nil {
		return nil, fmt.Errorf("decoding attachment %s: %w", a.Name, err)
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// AttachmentWrite is one attachment accompanying a document write.
// Data == nil marks a stub carried forward from the parent revision.
type AttachmentWrite struct {
	Name        string
	ContentType string
	Data        []byte
	// Replicated writes carry the source's bookkeeping. Zero values mean
	// "compute locally".
	RevPos int
	Digest string
	// Encoding marks Data as encoded (gzip). DecodedLength is then the
	// identity size the peer declared.
	Encoding      string
	DecodedLength int64
}

func AttachmentDigest(data []byte) string {
	sum := md5.Sum(data)
	return "md5-" + base64.StdEncoding.EncodeToString(sum[:])
}

// writeAttachments stores the attachments of a freshly inserted leaf.
// Stubs copy the parent revision's rows. Missing stubs return CouchDB's 412.
func writeAttachments(
	ctx context.Context,
	tx pgx.Tx,
	db *DB,
	id string,
	rev couch.Rev,
	parent *couch.Rev,
	atts []AttachmentWrite,
) error {
	for _, att := range atts {
		if att.Data == nil {
			if parent == nil {
				return missingStub(att.Name)
			}
			// A stub naming a revpos must name the right one (COUCHDB-809).
			revposCond := ""
			args := []any{id, rev.Num, rev.Hash, parent.Num, parent.Hash, att.Name}
			if att.RevPos > 0 {
				args = append(args, att.RevPos)
				revposCond = " AND revpos = $7"
			}
			tag, err := tx.Exec(ctx, fmt.Sprintf(
				`INSERT INTO %[1]s.attachments
				   (doc_id, rev_num, rev_hash, name, content_type, digest,
				    revpos, length, encoding, encoded_length, data)
				 SELECT doc_id, $2, $3, name, content_type, digest, revpos,
				        length, encoding, encoded_length, data
				 FROM %[1]s.attachments
				 WHERE doc_id = $1 AND rev_num = $4 AND rev_hash = $5 AND name = $6%s`,
				db.Schema, revposCond), args...)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return missingStub(att.Name)
			}
			continue
		}
		revpos := att.RevPos
		if revpos == 0 {
			revpos = rev.Num
		}
		digest := att.Digest
		if digest == "" {
			digest = AttachmentDigest(att.Data)
		}
		length := int64(len(att.Data))
		encodedLength := int64(0)
		if att.Encoding != "" {
			length = att.DecodedLength
			encodedLength = int64(len(att.Data))
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s.attachments
			   (doc_id, rev_num, rev_hash, name, content_type, digest,
			    revpos, length, encoding, encoded_length, data)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`, db.Schema),
			id, rev.Num, rev.Hash, att.Name, att.ContentType, digest,
			revpos, length, att.Encoding, encodedLength, att.Data,
		); err != nil {
			return err
		}
	}
	return nil
}

// revAttsForWrite assembles the attachment triples CouchDB folds into a
// revision hash, in write order. Stubs
// contribute the parent revision's stored content type and digest, like
// couch_doc:merge_stubs resolving from disk.
func revAttsForWrite(
	ctx context.Context,
	tx pgx.Tx,
	db *DB,
	id string,
	parent *couch.Rev,
	atts []AttachmentWrite,
) ([]couch.RevAtt, error) {
	if len(atts) == 0 {
		return nil, nil
	}
	out := make([]couch.RevAtt, 0, len(atts))
	for _, att := range atts {
		if att.Data == nil {
			if parent == nil {
				return nil, missingStub(att.Name)
			}
			revposCond := ""
			args := []any{id, parent.Num, parent.Hash, att.Name}
			if att.RevPos > 0 {
				args = append(args, att.RevPos)
				revposCond = " AND revpos = $5"
			}
			var ctype, digest string
			err := tx.QueryRow(ctx, fmt.Sprintf(
				`SELECT content_type, digest FROM %s.attachments
				 WHERE doc_id = $1 AND rev_num = $2 AND rev_hash = $3 AND name = $4%s`,
				db.Schema, revposCond), args...).Scan(&ctype, &digest)
			if err == pgx.ErrNoRows {
				return nil, missingStub(att.Name)
			}
			if err != nil {
				return nil, err
			}
			out = append(out, couch.RevAtt{Name: att.Name, ContentType: ctype, Digest: digest})
			continue
		}
		digest := att.Digest
		if digest == "" {
			digest = AttachmentDigest(att.Data)
		}
		out = append(out, couch.RevAtt{Name: att.Name, ContentType: att.ContentType, Digest: digest})
	}
	return out, nil
}

func missingStub(name string) error {
	return couch.NewError(412, "missing_stub",
		fmt.Sprintf("Invalid attachment stub in doc for %s", name))
}

const attachmentColumns = "name, content_type, digest, revpos, length, encoding, encoded_length"

// Attachments lists a revision's attachment metadata (no data).
// Ordered by the revision that introduced each attachment, then by name.
// Multipart responses list parts in write order. Carried-over stubs are older
// than later additions, which CouchDB clients depend on.
func (s *Store) Attachments(ctx context.Context, db *DB, id string, rev couch.Rev) ([]Attachment, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT %s FROM %s.attachments
		 WHERE doc_id = $1 AND rev_num = $2 AND rev_hash = $3 ORDER BY revpos, name`,
		attachmentColumns, db.Schema), id, rev.Num, rev.Hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var atts []Attachment
	for rows.Next() {
		var att Attachment
		if err := rows.Scan(&att.Name, &att.ContentType, &att.Digest,
			&att.RevPos, &att.Length, &att.Encoding, &att.EncodedLength); err != nil {
			return nil, err
		}
		atts = append(atts, att)
	}
	return atts, rows.Err()
}

// WinnerAttachments lists attachment metadata for many documents' winning
// revisions in one query, keyed by doc id (ids without attachments are
// absent). Batch companion to Attachments for _bulk_get.
func (s *Store) WinnerAttachments(ctx context.Context, db *DB, ids []string) (map[string][]Attachment, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT a.doc_id, a.name, a.content_type, a.digest, a.revpos,
		        a.length, a.encoding, a.encoded_length
		 FROM %s.attachments a
		 JOIN %s.docs d ON d.id = a.doc_id
		   AND d.rev_num = a.rev_num AND d.rev_hash = a.rev_hash
		 WHERE a.doc_id = ANY($1) ORDER BY a.doc_id, a.revpos, a.name`,
		db.Schema, db.Schema), ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]Attachment)
	for rows.Next() {
		var id string
		var att Attachment
		if err := rows.Scan(&id, &att.Name, &att.ContentType, &att.Digest,
			&att.RevPos, &att.Length, &att.Encoding, &att.EncodedLength); err != nil {
			return nil, err
		}
		out[id] = append(out[id], att)
	}
	return out, rows.Err()
}

// GetAttachment fetches one attachment of a revision, data included.
func (s *Store) GetAttachment(ctx context.Context, db *DB, id string, rev couch.Rev, name string) (*Attachment, error) {
	att := &Attachment{}
	err := s.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT %s, data FROM %s.attachments
		 WHERE doc_id = $1 AND rev_num = $2 AND rev_hash = $3 AND name = $4`,
		attachmentColumns, db.Schema), id, rev.Num, rev.Hash, name,
	).Scan(&att.Name, &att.ContentType, &att.Digest, &att.RevPos,
		&att.Length, &att.Encoding, &att.EncodedLength, &att.Data)
	if err == pgx.ErrNoRows {
		return nil, couch.NewError(404, "not_found", "Document is missing attachment")
	}
	if err != nil {
		return nil, err
	}
	return att, nil
}

// AttachmentsWithData loads all of a revision's attachments including bytes.
func (s *Store) AttachmentsWithData(ctx context.Context, db *DB, id string, rev couch.Rev) ([]Attachment, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(
		`SELECT %s, data FROM %s.attachments
		 WHERE doc_id = $1 AND rev_num = $2 AND rev_hash = $3 ORDER BY revpos, name`,
		attachmentColumns, db.Schema), id, rev.Num, rev.Hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var atts []Attachment
	for rows.Next() {
		var att Attachment
		if err := rows.Scan(&att.Name, &att.ContentType, &att.Digest,
			&att.RevPos, &att.Length, &att.Encoding, &att.EncodedLength,
			&att.Data); err != nil {
			return nil, err
		}
		atts = append(atts, att)
	}
	return atts, rows.Err()
}
