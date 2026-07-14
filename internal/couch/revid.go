package couch

// CouchDB's revision hash, reproduced byte-for-byte.
//
// CouchDB computes (couch_db:new_revid/1):
//
//	md5(term_to_binary([Deleted, OldStart, OldRev, Body, Atts2],
//	                   [{minor_version, 1}]))
//
// where Body is the jiffy-decoded EJSON body (JSON member order preserved,
// meta underscore fields stripped per couch_doc:transfer_fields), OldRev is
// the parent's hash as a 16-byte binary (hex-decoded when it is 32 hex
// chars, raw bytes otherwise, 0 for a first revision), and Atts2 is the
// reversed list of {Name, Type, Md5} attachment triples (md5 over the
// stored form, possibly gzip-encoded).
//
// Matching it means reimplementing two Erlang behaviors:
//
//  1. jiffy's decode: objects as ordered {proplist} tuples, "1" an integer
//     but "1.0"/"1e2" floats, strings as UTF-8 binaries.
//  2. term_to_binary's external term format, including the quirk that a
//     list whose elements are all integers 0-255 encodes as STRING_EXT.
//
// Neither is specified, so TestLiveCouchRevIDs fuzzes this implementation
// against a real CouchDB.

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// RevAtt is one attachment's contribution to a revision hash, in document
// order. Digest is the stored form's "md5-<base64>" digest.
type RevAtt struct {
	Name        string
	ContentType string
	Digest      string
}

// NewRevID computes the CouchDB revision hash for an interactive write.
// rawDoc is the document JSON as received, with member order intact.
// Underscore meta fields are stripped here, mirroring couch_doc:transfer_fields.
// server-assembled bodies pass a canonical marshal instead.
func NewRevID(deleted bool, parent *Rev, rawDoc []byte, atts []RevAtt) (string, error) {
	body, err := ejsonBody(rawDoc)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	buf.WriteByte(131) // external term format version
	// The outer 5-element list holds atoms/tuples, never all-small-ints.
	buf.WriteByte(108) // LIST_EXT
	writeU32(&buf, 5)
	etfAtom(&buf, boolAtom(deleted))
	etfInt(&buf, big.NewInt(int64(oldStart(parent))))
	if err := etfOldRev(&buf, parent); err != nil {
		return "", err
	}
	if err := etfValue(&buf, body); err != nil {
		return "", err
	}
	if err := etfAtts(&buf, atts); err != nil {
		return "", err
	}
	buf.WriteByte(106) // NIL_EXT tail

	sum := md5.Sum(buf.Bytes())
	return hex.EncodeToString(sum[:]), nil
}

// NextRev is NewRevID plus the generation bump.
func NextRev(deleted bool, parent *Rev, rawDoc []byte, atts []RevAtt) (Rev, error) {
	hash, err := NewRevID(deleted, parent, rawDoc, atts)
	if err != nil {
		return Rev{}, err
	}
	num := 1
	if parent != nil {
		num = parent.Num + 1
	}
	return Rev{Num: num, Hash: hash}, nil
}

func boolAtom(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func oldStart(parent *Rev) int {
	if parent == nil {
		return 0
	}
	return parent.Num
}

// etfOldRev writes the parent hash the way couch_doc:parse_revid holds it:
// 32 hex chars become the raw 16 bytes. Anything else stays ASCII. A first
// revision contributes the integer 0.
func etfOldRev(buf *bytes.Buffer, parent *Rev) error {
	if parent == nil {
		etfInt(buf, big.NewInt(0))
		return nil
	}
	hash := []byte(parent.Hash)
	if len(parent.Hash) == 32 && isHexString(parent.Hash) {
		var err error
		if hash, err = hex.DecodeString(parent.Hash); err != nil {
			return err
		}
	}
	etfBinary(buf, hash)
	return nil
}

func isHexString(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// etfAtts writes the attachment triples, reversed: couch_db folds the att
// list with a prepend.
func etfAtts(buf *bytes.Buffer, atts []RevAtt) error {
	if len(atts) == 0 {
		buf.WriteByte(106) // NIL_EXT
		return nil
	}
	buf.WriteByte(108) // LIST_EXT: tuples, never the STRING_EXT case
	writeU32(buf, uint32(len(atts)))
	for i := len(atts) - 1; i >= 0; i-- {
		att := atts[i]
		digest, err := base64.StdEncoding.DecodeString(
			strings.TrimPrefix(att.Digest, "md5-"))
		if err != nil {
			return fmt.Errorf("attachment %s digest: %w", att.Name, err)
		}
		buf.WriteByte(104) // SMALL_TUPLE_EXT
		buf.WriteByte(3)
		etfBinary(buf, []byte(att.Name))
		etfBinary(buf, []byte(att.ContentType))
		etfBinary(buf, digest)
	}
	buf.WriteByte(106) // NIL_EXT tail
	return nil
}

// ---------------------------------------------------------------------------
// EJSON: JSON decoded the way jiffy hands it to Erlang.
// ---------------------------------------------------------------------------

// ejMember is one member of an EJSON object (order preserved, duplicates
// kept because jiffy proplists have both properties).
type ejMember struct {
	key string
	val any
}

// ejObject distinguishes objects from arrays in the value tree. Values are
// nil, bool, *big.Int, float64, string, []any, or ejObject.
type ejObject []ejMember

// ejsonBody parses a full document's raw JSON into the EJSON body term:
// member order kept, meta underscore fields dropped exactly like
// couch_doc:transfer_fields (which keeps _access and _replication_* in the
// body and moves/ignores the rest).
func ejsonBody(rawDoc []byte) (ejObject, error) {
	dec := json.NewDecoder(bytes.NewReader(rawDoc))
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, BadRequest("invalid document JSON")
	}
	obj, err := ejsonObject(dec)
	if err != nil {
		return nil, err
	}
	body := make(ejObject, 0, len(obj))
	for _, m := range obj {
		if isMetaField(m.key) {
			continue
		}
		body = append(body, m)
	}
	return body, nil
}

// isMetaField reports underscore members that never reach the hashed body.
// _access and _replication_* stay because transfer_fields keeps them in the body.
// anything else starting with _ would have been rejected as a bad special
// member before hashing.
func isMetaField(key string) bool {
	switch key {
	case "_id", "_rev", "_attachments", "_revisions", "_deleted",
		"_revs_info", "_local_seq", "_conflicts", "_deleted_conflicts":
		return true
	}
	return false
}

// ejsonObject consumes members up to and including the closing brace.
func ejsonObject(dec *json.Decoder) (ejObject, error) {
	var obj ejObject
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := keyTok.(string)
		if !ok {
			return nil, fmt.Errorf("ejson: non-string key %v", keyTok)
		}
		val, err := ejsonValue(dec)
		if err != nil {
			return nil, err
		}
		obj = append(obj, ejMember{key: key, val: val})
	}
	if _, err := dec.Token(); err != nil { // closing }
		return nil, err
	}
	return obj, nil
}

func ejsonValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return ejsonObject(dec)
		case '[':
			var arr []any
			for dec.More() {
				v, err := ejsonValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, v)
			}
			if _, err := dec.Token(); err != nil { // closing ]
				return nil, err
			}
			return arr, nil
		}
		return nil, fmt.Errorf("ejson: unexpected delimiter %v", t)
	case json.Number:
		return ejsonNumber(t)
	case string, bool, nil:
		return t, nil
	}
	return nil, fmt.Errorf("ejson: unexpected token %v", tok)
}

// ejsonNumber mirrors jiffy: a literal with '.', 'e', or 'E' is an Erlang
// float, anything else an (arbitrary-precision) integer.
func ejsonNumber(n json.Number) (any, error) {
	s := n.String()
	if strings.ContainsAny(s, ".eE") {
		f, err := n.Float64()
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	i, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("ejson: bad integer %q", s)
	}
	return i, nil
}

// ---------------------------------------------------------------------------
// External term format (term_to_binary, minor_version 1).
// ---------------------------------------------------------------------------

func writeU32(buf *bytes.Buffer, n uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	buf.Write(b[:])
}

// etfAtom writes ATOM_EXT, term_to_binary's default atom encoding. The
// newer UTF-8 forms are negotiated on the distribution protocol only, which
// is why CouchDB's famous empty-doc rev has survived every OTP upgrade).
func etfAtom(buf *bytes.Buffer, name string) {
	buf.WriteByte(100) // ATOM_EXT
	buf.WriteByte(byte(len(name) >> 8))
	buf.WriteByte(byte(len(name)))
	buf.WriteString(name)
}

func etfBinary(buf *bytes.Buffer, data []byte) {
	buf.WriteByte(109) // BINARY_EXT
	writeU32(buf, uint32(len(data)))
	buf.Write(data)
}

func etfInt(buf *bytes.Buffer, n *big.Int) {
	if n.IsInt64() {
		v := n.Int64()
		switch {
		case v >= 0 && v <= 255:
			buf.WriteByte(97) // SMALL_INTEGER_EXT
			buf.WriteByte(byte(v))
			return
		case v >= math.MinInt32 && v <= math.MaxInt32:
			buf.WriteByte(98) // INTEGER_EXT
			writeU32(buf, uint32(int32(v)))
			return
		}
	}
	// SMALL_BIG_EXT: sign byte + little-endian magnitude.
	mag := new(big.Int).Abs(n).Bytes() // big-endian
	buf.WriteByte(110)
	buf.WriteByte(byte(len(mag)))
	if n.Sign() < 0 {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	for i := len(mag) - 1; i >= 0; i-- {
		buf.WriteByte(mag[i])
	}
}

func etfFloat(buf *bytes.Buffer, f float64) {
	buf.WriteByte(70) // NEW_FLOAT_EXT (minor_version 1)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
	buf.Write(b[:])
}

func etfValue(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case nil:
		etfAtom(buf, "null")
	case bool:
		etfAtom(buf, boolAtom(t))
	case *big.Int:
		etfInt(buf, t)
	case float64:
		etfFloat(buf, t)
	case string:
		etfBinary(buf, []byte(t))
	case ejObject:
		// {Proplist}: a 1-tuple around the member list.
		buf.WriteByte(104) // SMALL_TUPLE_EXT
		buf.WriteByte(1)
		if len(t) == 0 {
			buf.WriteByte(106) // NIL_EXT
			return nil
		}
		buf.WriteByte(108) // LIST_EXT: 2-tuples, never all-small-ints
		writeU32(buf, uint32(len(t)))
		for _, m := range t {
			buf.WriteByte(104)
			buf.WriteByte(2)
			etfBinary(buf, []byte(m.key))
			if err := etfValue(buf, m.val); err != nil {
				return err
			}
		}
		buf.WriteByte(106) // NIL_EXT tail
	case []any:
		return etfList(buf, t)
	default:
		return fmt.Errorf("ejson: unencodable value %T", v)
	}
	return nil
}

// etfList encodes a JSON array and uses term_to_binary's STRING_EXT
// optimization. A list whose elements are all integers 0-255 (and at most
// 65535 long) travels as a byte string.
func etfList(buf *bytes.Buffer, list []any) error {
	if len(list) == 0 {
		buf.WriteByte(106) // NIL_EXT
		return nil
	}
	if len(list) <= 65535 {
		bytesOnly := true
		for _, v := range list {
			n, ok := v.(*big.Int)
			if !ok || !n.IsInt64() || n.Int64() < 0 || n.Int64() > 255 {
				bytesOnly = false
				break
			}
		}
		if bytesOnly {
			buf.WriteByte(107) // STRING_EXT
			buf.WriteByte(byte(len(list) >> 8))
			buf.WriteByte(byte(len(list)))
			for _, v := range list {
				buf.WriteByte(byte(v.(*big.Int).Int64()))
			}
			return nil
		}
	}
	buf.WriteByte(108) // LIST_EXT
	writeU32(buf, uint32(len(list)))
	for _, v := range list {
		if err := etfValue(buf, v); err != nil {
			return err
		}
	}
	buf.WriteByte(106) // NIL_EXT tail
	return nil
}
