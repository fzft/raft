package storage

import "github.com/tinylib/msgp/msgp"

type Item struct {
	Key     string
	version int
	data    []byte
	lock    *LockInfo
}

func NewItemData(key string, data []byte) *Item {
	return &Item{
		Key:  key,
		data: data,
	}
}

func NewItemRead(mr *msgp.Reader, fileVersion int) (*Item, error) {
	var (
		err error
		ok  bool
	)
	i := new(Item)
	i.Key, err = mr.ReadString()
	size, err := mr.ReadInt()
	if size > 0 {
		i.data = make([]byte, size)
		mr.ReadExactBytes(i.data)
	}
	i.version, err = mr.ReadInt()
	if ok, err = mr.ReadBool(); ok {
		var (
			expiry int64
			owner  string
		)
		expiry, err = mr.ReadInt64()
		owner, err = mr.ReadString()
		i.lock = NewLockInfo(expiry, owner)
	}
	return i, err
}

func NewItem(key string) *Item {
	return &Item{Key: key}
}

func (i *Item) Write(mw *msgp.Writer) {
	mw.WriteString(i.Key)
	mw.WriteInt(len(i.data))
	if len(i.data) > 0 {
		mw.Write(i.data)
	}
	mw.WriteInt(i.version)
	mw.WriteBool(i.lock != nil)
	if i.lock != nil {
		mw.WriteInt64(i.lock.expiry)
		mw.WriteString(i.lock.owner)
	}
}

type LockInfo struct {
	expiry int64
	owner  string
}

func NewLockInfo(expiry int64, owner string) *LockInfo {
	return &LockInfo{
		expiry: expiry,
		owner:  owner,
	}
}

func (l LockInfo) copy() *LockInfo {
	return NewLockInfo(l.expiry, l.owner)
}
