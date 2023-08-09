package com.damon.dataframe.examples

class Util extends Serializable {
  def combine(fname: String, mname: String, lname: String): String = {
    fname+","+mname+","+lname
  }
}
