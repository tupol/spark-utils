/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package org.tupol.spark

import java.nio.ByteBuffer

/**
 * Utilities that help with byte serialization
 */
package object byteable {

  /**
   * Trait that helps classes to be serialised to a byte array.
   */
  trait Byteable extends Any {
    def toByteArray: Array[Byte]
  }

  implicit class XShort(val x: Short) extends AnyVal with Byteable {
    def toByteArray: Array[Byte] = {
      val buf = ByteBuffer.allocate(2)
      buf.putShort(x)
      buf.array
    }
  }

  implicit class XInt(val x: Int) extends AnyVal with Byteable {
    def toByteArray: Array[Byte] = {
      val buf = ByteBuffer.allocate(4)
      buf.putInt(x)
      buf.array
    }
  }

  implicit class XLong(val x: Long) extends AnyVal with Byteable {
    def toByteArray: Array[Byte] = {
      val buf = ByteBuffer.allocate(8)
      buf.putLong(x)
      buf.array
    }
  }

  implicit class XFloat(val x: Float) extends AnyVal with Byteable {
    def toByteArray: Array[Byte] = {
      val buf = ByteBuffer.allocate(4)
      buf.putFloat(x)
      buf.array
    }
  }

  implicit class XDouble(val x: Double) extends AnyVal with Byteable {
    def toByteArray: Array[Byte] = {
      val buf = ByteBuffer.allocate(8)
      buf.putDouble(x)
      buf.array
    }
  }

}

