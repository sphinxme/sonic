/*
 * Copyright 2021 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ast

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// support decode/encode ast.Node to/from bson

// contains bson encoder/decoders for golang type
var defaultBSONCodecRegistry = buildDefaultBSONCodecRegistry()

func buildDefaultBSONCodecRegistry() *bsoncodec.Registry {
	rb := bsoncodec.NewRegistryBuilder()
	defaultValueEncoders := bsoncodec.DefaultValueEncoders{}
	defaultValueDecoders := bsoncodec.DefaultValueDecoders{}
	defaultValueEncoders.RegisterDefaultEncoders(rb)
	defaultValueDecoders.RegisterDefaultDecoders(rb)
	return rb.Build()
}

var fallbackBSONNumberEncoder = bsoncodec.ValueEncoderFunc(bsoncodec.DefaultValueEncoders{}.JSONNumberEncodeValue)

func encodeBSON(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, node Node) error {
	switch t := node.Type(); t {
	case V_NONE:
		return vw.WriteUndefined() // 实际上不应该走到这一步
	case V_ERROR:
		return node
	case V_NULL:
		return vw.WriteNull()
	case V_TRUE:
		return vw.WriteBoolean(true)
	case V_FALSE:
		return vw.WriteBoolean(false)
	case V_ARRAY:
		aw, err := vw.WriteArray()
		if err != nil {
			return err
		}

		iter, err := node.Values()
		if err != nil {
			return err
		}

		currNode := Node{}
		for iter.HasNext() {
			iter.Next(&currNode)
			vw, err := aw.WriteArrayElement()
			if err != nil {
				return err
			}

			if err := encodeBSON(ec, vw, currNode); err != nil {
				return err
			}
		}
		return aw.WriteArrayEnd()
	case V_OBJECT:
		dw, err := vw.WriteDocument()
		if err != nil {
			return err
		}

		iter, err := node.Properties()
		if err != nil {
			return err
		}

		p := Pair{}
		for iter.HasNext() {
			iter.Next(&p)
			vw, err := dw.WriteDocumentElement(p.Key)
			if err != nil {
				return err
			}

			if err := encodeBSON(ec, vw, p.Value); err != nil {
				return err
			}
		}
		return dw.WriteDocumentEnd()
	case V_STRING:
		str, err := node.String()
		if err != nil {
			return err
		}
		return vw.WriteString(str)
	case V_NUMBER:
		number, err := node.StrictNumber()
		if err != nil {
			return err
		}
		val := reflect.ValueOf(number)
		encoder, err := ec.LookupEncoder(val.Type())
		if _, ok := err.(bsoncodec.ErrNoEncoder); ok {
			encoder = fallbackBSONNumberEncoder
		}
		if err != nil && encoder == nil {
			return err
		}

		if err := encoder.EncodeValue(ec, vw, val); err != nil {
			return err
		}
	case V_ANY:
		i, err := node.InterfaceUseNumber()
		if err != nil {
			return err
		}
		val := reflect.ValueOf(i)
		encoder, err := ec.LookupEncoder(val.Type())
		if err != nil {
			return err
		}
		if err := encoder.EncodeValue(ec, vw, val); err != nil {
			return err
		}

	}
	return nil
}

func decodeBSON(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val *Node) error {
	if val == nil {
		return errors.New("xxx")
	}
	node := Node{}
	switch vrType := vr.Type(); vrType {
	case bsontype.Double:
		num, err := vr.ReadDouble()
		if err != nil {
			return err
		}
		node = NewNumber(fmt.Sprint(num))
	case bsontype.String:
		str, err := vr.ReadString()
		if err != nil {
			return err
		}
		node = NewString(str)
	case bsontype.Array:
		var elems []Node
		ar, err := vr.ReadArray()
		if err != nil {
			return err
		}

		idx := 0
		for {
			vr, err := ar.ReadValue()
			if err == bsonrw.ErrEOA {
				break
			}
			if err != nil {
				return err
			}
			node := Node{}
			if err := decodeBSON(dc, vr, &node); err != nil {
				return err // todo: 标记上出错的上index
			}
			elems = append(elems, node)
			idx++
		}
		node = NewArray(elems)
	case bsontype.Binary:
		// json中没有bin这种类型 所以按照json.RawMessage即[]byte处理
		data, _, err := vr.ReadBinary()
		// todo:根据bson标准 以subtype分别处理
		if err != nil {
			return err
		}
		node = NewRaw(string(data))
	// case bsontype.Type(0):

	case bsontype.Undefined, bsontype.Null:
		node = NewNull()
	case bsontype.ObjectID:
		id, err := vr.ReadObjectID()
		if err != nil {
			return err
		}
		node = NewString(id.Hex())
	case bsontype.Boolean:
		b, err := vr.ReadBoolean()
		if err != nil {
			return err
		}
		node = NewBool(b)
	case bsontype.DateTime:
		ts, err := vr.ReadDateTime()
		if err != nil {
			return err
		}
		t := time.UnixMilli(ts)
		node = NewString(t.Format(time.RFC3339))
	case bsontype.Int32:
		i, err := vr.ReadInt32()
		if err != nil {
			return err
		}
		node = NewNumber(strconv.FormatInt(int64(i), 10))
	case bsontype.Timestamp:
		ts, ordinal, err := vr.ReadTimestamp() // Timestamp仅用于mongodb 照理说不应该出现在这里 // ordinal非nsec 而是一个保证时间戳唯一的值
		if err != nil {
			return err
		}
		t := time.Unix(int64(ts), int64(ordinal))
		node = NewString(t.Format(time.RFC3339))
	case bsontype.Int64:
		i, err := vr.ReadInt64()
		if err != nil {
			return err
		}
		node = NewNumber(strconv.FormatInt(i, 10))
	case bsontype.Decimal128:
		bigInt, err := vr.ReadDecimal128()
		if err != nil {
			return err
		}
		node = NewNumber(bigInt.String())
	case bsontype.EmbeddedDocument:
		var pairs []Pair
		dr, err := vr.ReadDocument()
		if err != nil {
			return err
		}

		for {
			key, vr, err := dr.ReadElement()
			if err == bsonrw.ErrEOD {
				break
			}
			if err != nil {
				return err
			}
			pair := Pair{Key: key}
			if err := decodeBSON(dc, vr, &pair.Value); err != nil {
				return err // todo: 标记上出错的上key
			}
			pairs = append(pairs, pair)
		}
		node = NewObject(pairs)
	// case bsontype.DBPointer, bsontype.JavaScript, bsontype.Symbol,
	// 	bsontype.CodeWithScope, bsontype.MinKey, bsontype.MaxKey, bsontype.Regex:
	//
	default:
		return errors.New("unsupported type")
	}
	*val = node
	return nil
}

func (self Node) MarshalBSON() ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	vw, err := bsonrw.NewBSONValueWriter(buf)
	if err != nil {
		return nil, err
	}

	ec := bsoncodec.EncodeContext{Registry: defaultBSONCodecRegistry}
	if err := encodeBSON(ec, vw, self); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (self *Node) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
	switch t {
	case bsontype.Binary:
		*self = NewRaw(string(data[1:]))
	default:
		vr := bsonrw.NewBSONValueReader(t, data)
		dc := bsoncodec.DecodeContext{Registry: defaultBSONCodecRegistry}
		err := decodeBSON(dc, vr, self)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (self *Node) UnmarshalBSON(data []byte) error {
	if len(data) < 5 {
		return errors.New("invalid bson doc size")
	}

	t := data[4] // bson type or bson binData subtype

	switch t {
	case bsontype.BinaryGeneric:
		*self = NewRaw(string(data[1:]))
	default:
		vr := bsonrw.NewBSONValueReader(bsontype.Type(t), data)
		dc := bsoncodec.DecodeContext{Registry: defaultBSONCodecRegistry}
		err := decodeBSON(dc, vr, self)
		if err != nil {
			panic(err)
		}
	}

	return nil
}
