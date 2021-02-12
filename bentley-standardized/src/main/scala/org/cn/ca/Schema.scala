package org.cn.ca

case class Schema(tables: Seq[Table])
case class Table(name:String,cols:Seq[Col],isOwerwrite:Boolean)
case class Col(name:String,colType:String,isPK:Boolean,hiveName:String,format:String)