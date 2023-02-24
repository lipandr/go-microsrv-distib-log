package log

import (
	"io"
	"os"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type Index struct {
	file *os.File
	mMap MMap
	size uint64
}

func NewIndex(f *os.File, c Config) (*Index, error) {
	idx := &Index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mMap, err = Map(
		idx.file.Fd(),
		PROTRead|PROTWrite,
		MapShared,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes in an offset and returns the associated record's position in the store.
// The given offset is relative to the segment's base offset;
// 0 is always the first record in the segment, 1 is the second, and so on.
func (i *Index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mMap[pos : pos+entWidth])
	pos = enc.Uint64(i.mMap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the Index.
// First, we validate that we have space to write the record.
// If there's space, we encode the offset and position and write them to the memory-mapped file.
// Then we increment the position where the next write will take place.
func (i *Index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mMap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mMap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mMap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

// Close makes sure the memory-mapped file has synced its data to the persisted file
// and that persisted file has flushed its contents to stable storage.
func (i *Index) Close() error {
	if err := i.mMap.Sync(MsSync); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Name returns the Index's file path.
func (i *Index) Name() string {
	return i.file.Name()
}
