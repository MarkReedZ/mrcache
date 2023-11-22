
#// 26 bits block id, 8 bits shift distance, 4 bits last key byte, 5 bits storage, 21 bits value

def prt(b):
  print( " block id ", b >> 38 )
  print( " shift    ", (b >> 30) & 0xFF )
  print( " key      ", (b >> 26) & 0xF )
  print( " storage  ", (b >> 21) & 0x1F )
  print( " value    ", (b >>  0) & 0x1FFFFF )


prt(0x0000000098165ead)
prt(0x00000000D8165ead)
