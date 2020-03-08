package com.atguigu.bigdata.flink
import  util.control.Breaks._

object QuickSortDemo {


  def quickSort(left: Int, right:Int,arr:Array[Int]): Unit = {

    var l =left
    var r = right

    var pivot=arr((left +right) /2)
    var temp =0
    breakable {
      while (l < r) {
        while (arr(l) < pivot) {
          l += 1
        }
        while (arr(r) > pivot) {
          r -= 1
        }
        if (l >= r) {
          break()
        }
        var temp = arr(l)
        arr(l) = arr(r)
        arr(r) = temp

        if (arr(l) == pivot) {
          r -= 1
        }
        if (arr(r) == pivot) {
          r += 1
        }
      }
    }
    if(l == r ){
      l+=1
      r-=1
    }
      if(left < r ){
        quickSort(left ,r , arr)
        }
      if(right >l){
        quickSort(l,right,arr)
      }
    }
}