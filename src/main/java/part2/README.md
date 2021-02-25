# Part 2
This part tries to tackle GeoSpark problems.

## Challenge 1
Find the total number of ships in the dataset, the number of points per a ship, the max and min latitude, longitude and timestamp and mean sampling period. 
This challenge does not create a file, it rather prints its output:
```
Ship count: 44

Ship 209465000 data count: 344
	Average sampling period: 267.4418604651163

Ship 237123000 data count: 49
	Average sampling period: 1224.4897959183672

Ship 237482000 data count: 1631
	Average sampling period: 18.393623543838135

Ship 241065000 data count: 1054
	Average sampling period: 18.026565464895636

Ship 237534000 data count: 38
	Average sampling period: 20263.157894736843

Ship 237046000 data count: 212
	Average sampling period: 47.16981132075472

Ship 240280000 data count: 138
	Average sampling period: 572.463768115942

Ship 237562000 data count: 20375
	Average sampling period: 0.44171779141104295

Ship 239131000 data count: 1337
	Average sampling period: 15.706806282722512

Ship 237526000 data count: 5981
	Average sampling period: 12.038120715599398

Ship 240430000 data count: 99
	Average sampling period: 292.92929292929296

Ship 239128000 data count: 2494
	Average sampling period: 116.27906976744185

Ship 237805000 data count: 102
	Average sampling period: 2441.176470588235

Ship 239117000 data count: 8579
	Average sampling period: 10.3741694836228

Ship 237511000 data count: 2456
	Average sampling period: 13.029315960912053

Ship 239139000 data count: 4738
	Average sampling period: 8.23132123258759

Ship 237591000 data count: 78
	Average sampling period: 128.2051282051282

Ship 237540000 data count: 1035
	Average sampling period: 145.89371980676327

Ship 237959000 data count: 9
	Average sampling period: 29000.0

Ship 239186000 data count: 40
	Average sampling period: 4500.0

Ship 237119000 data count: 401
	Average sampling period: 74.81296758104739

Ship 237565000 data count: 8
	Average sampling period: 6250.0

Ship 239178000 data count: 2380
	Average sampling period: 3.7815126050420167

Ship 241063000 data count: 601
	Average sampling period: 51.58069883527454

Ship 239538000 data count: 165
	Average sampling period: 0.0

Ship 237525000 data count: 20
	Average sampling period: 0.0

Ship 239101000 data count: 32
	Average sampling period: 1218.75

Ship 239240000 data count: 654
	Average sampling period: 246.17737003058105

Ship 240370000 data count: 302350
	Average sampling period: 0.0330742516950554

Ship 237536000 data count: 26
	Average sampling period: 346.15384615384613

Ship 239383000 data count: 51
	Average sampling period: 196.07843137254903

Ship 239901000 data count: 1761
	Average sampling period: 34.07155025553663

Ship 240150000 data count: 39
	Average sampling period: 1769.2307692307693

Ship 239174000 data count: 9609
	Average sampling period: 78.05182641273805

Ship 240304000 data count: 4295
	Average sampling period: 2.561117578579744

Ship 239100000 data count: 146
	Average sampling period: 273.972602739726

Ship 240153000 data count: 2923
	Average sampling period: 44.13274033527198

Ship 239272000 data count: 420
	Average sampling period: 1304.7619047619048

Ship 239324000 data count: 1063
	Average sampling period: 112.88805268109125

Ship 237920000 data count: 993
	Average sampling period: 119.83887210473313

Ship 237656000 data count: 401
	Average sampling period: 27.43142144638404

Ship 237418000 data count: 140
	Average sampling period: 2571.4285714285716

Ship 241091000 data count: 26371
	Average sampling period: 1.1755337302339692

Ship 248847000 data count: 1093
	Average sampling period: 163.7694419030192

Max longitude: 23.66821
Min longitude: 0.0
Max latitude: 37.962445
Min latitude: 0.0
Max timestamp: 1527627599000
Min timestamp: 0
```

## Challenge 2
Remove outliers, where the speed of a ship is more than 100 kmh. This challenge does create a file with its output. It outputs the id of the ship, and a list of the speeds it was found travelling with.

### The output of all the challenges can be found under the directory `/output/part2`