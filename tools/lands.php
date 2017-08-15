<?php
$codes = array();
$fh = fopen(__DIR__ . '/codes.csv', 'r');
$title = fgetcsv($fh, 2048);
while($line = fgetcsv($fh, 2048)) {
  $line[2] = trim($line[2]);
  $codes[$line[3]] = $line[4];
}

$geo = json_decode(file_get_contents(__DIR__ . '/土地.json'), true);
$lands = array();
foreach($geo['features'] AS $f) {
  $section = $codes[$f['properties']['SECTCODE']];
  if(!isset($lands[$section])) {
    $lands[$section] = array();
  }
  $lands[$section][$f['properties']['LANDCODE']] = $f;
}
$lands['延吉段一小段'][243] = json_decode('{"type":"Feature","geometry":{"type":"MultiPolygon","coordinates":[[[[121.55581528862,25.047435896108],[121.55585722153,25.047441324527],[121.55583097729,25.047619889991],[121.55578911675,25.047614527004],[121.55581528862,25.047435896108]]]]},"properties":{"\u7e23\u5e02":"\u81fa\u5317\u5e02","\u9109\u93ae":"\u677e\u5c71\u5340","\u6bb5\u540d":"\u5ef6\u5409\u6bb5\u4e00\u5c0f\u6bb5","\u5730\u865f":2430000,"ymax":25.047619889991,"ymin":25.047435896108,"xmax":121.55585722153,"xmin":121.55578911675,"xcenter":121.55582316914,"ycenter":25.047527893049,"area_id":"AD","section_id":"0609","land_id":2430000}}', true);
$fh = fopen(__DIR__ . '/lands_utf8.csv', 'r');
$titles = fgetcsv($fh, 2048);
$valueLines = array();
while($line = fgetcsv($fh, 2048)) {
  $line = array_combine($titles, $line);
  if($line['縣市'] === '新北市') {
    continue;
  }
  //print_r($line);
  $section = trim("{$line['段']}段");
  if(substr($section, -6) === '段段') {
    $section = substr($section, 0, -3);
  }
  if(!empty($line['小段'])) {
    $line['小段'] = str_replace('小段', '', $line['小段']);
    switch($line['小段']) {
      case '1':
      $line['小段'] = '一';
      break;
      case '2':
      $line['小段'] = '二';
      break;
    }
    $section .= trim("{$line['小段']}小段");
  }
  if(!isset($lands[$section])) {
    echo $section;
    print_r($line);
    exit();
  }
  $parts = explode('-', $line['地號']);
  if(count($parts) === 2) {
    $line['地號'] = intval($parts[0]);
    $parts[1] = intval($parts[1]);
    if($parts[1] > 0) {
      $line['地號'] .= '-' . $parts[1];
    }
  }
  unset($line['']);
  if(!isset($lands[$section][$line['地號']])) {
    print_r(array_keys($lands[$section]));
    print_r($line['地號']);
    print_r($line);
    exit();
  }
  $jsonText = addslashes(json_encode($lands[$section][$line['地號']]));
  $detail = '';
  foreach($line AS $k => $v) {
    $detail .= "[{$k}]{$v}\n";
  }
  $detail = addslashes($detail);
  $valueLines[] = "('{$line['鄉鎮市區']}區', '{$section}', '{$line['地號']}', '{$line['地號']}', '{$section}', '{$line['地號']}', '{$line['面積(m2)']}', '臺北市政府{$line['機關別']}', '1', NULL, '{$jsonText}', '', '{$detail}', '')";
}

$geo = json_decode(file_get_contents(__DIR__ . '/建物座落.json'), true);

foreach($geo['features'] AS $f) {
  $section = $codes[$f['properties']['SECTCODE']];
  $jsonText = addslashes(json_encode($f));
  $valueLines[] = "('', '{$section}', '{$f['properties']['LANDCODE']}', '{$f['properties']['LANDCODE']}', '{$section}', '{$f['properties']['LANDCODE']}', '{$f['properties']['CALAREA']}', '臺北市政府', '1', NULL, '{$jsonText}', '', '', '')";
}

$geo = json_decode(file_get_contents(__DIR__ . '/建物輪廓.json'), true);
foreach($geo['features'] AS $f) {
  $jsonText = addslashes(json_encode($f));
  $valueLines[] = "('', '', '', '', '', '', '', '臺北市政府', '1', NULL, '{$jsonText}', '', '', '')";
}

file_put_contents(__DIR__ . '/import.sql', "INSERT INTO taipei_pop (block, road, road_no, land_no, ser_no01, ser_no02, area, unit, id, locations, geo_json, renew_status, renew_detail, upload_image) VALUES\n" . implode(",\n", $valueLines));
