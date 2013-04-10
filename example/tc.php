<?php
//$ob = new Oceanbase("10.232.36.192",2500);
$ob = new Oceanbase("10.246.133.210",2500);

$item1 = array('item_collector_count'=>array('int'=>3),
                'item_picurl' => array('char'=>'3'),
                'item_ends' => array('date'=> 3)
              );
$item3 = array ('info_is_shared' => array('int'=>2),
                'info_user_nick' => array('char'=>'2'),
                'info_collect_time' => array ('date'=>2)
                );

$insert_items = array('collect_item_etao' => array ('AAAAAAAAA'=> $item1, 'AAAAAAAAB'=> $item1), 
                      'collect_info_etao' => array('AAAAAAAAAAAAAAAAA'=> $item3) 
                        );
$result = $ob->minsert($insert_items);
var_dump($result);



$items = array ( 'collect_item_etao' => array('AAAAAAAAA'=> array("item_collector_count","item_ends", "item_picurl"), 
					 'AAAAAAAAB'=> array("item_collector_count","item_ends", "item_picurl")),
                 'collect_info_etao' => array('AAAAAAAAAAAAAAAAA'=> array('info_is_shared', 'info_user_nick', 'info_collect_time')
                                        )
                 );
$uuu = array('AAAAAAAAA'=> array("item_collector_count","item_ends", "item_picurl"),
                                         'AAAAAAAAB'=> array("item_collector_count","item_ends", "item_picurl"));
$result = $ob->mget($items);
var_dump($result);


$items = array ('collect_item_etao' => array ('AAAAAAAAB', 'AAAAAAAAA'),
		'collect_info_etao' => array('AAAAAAAAAAAAAAAAA')
		);
$result = $ob->mdelete($items);
var_dump ($result);
$items = array ( 'collect_item_etao' => array('AAAAAAAAA'=> array("item_collector_count","item_ends", "item_picurl"),
                                         'AAAAAAAAB'=> array("item_collector_count","item_ends", "item_picurl")),
                 'collect_info_etao' => array('AAAAAAAAAAAAAAAAA'=> array('info_is_shared', 'info_user_nick', 'info_collect_time')
                                        )
                 );
$uuu = array('AAAAAAAAA'=> array("item_collector_count","item_ends", "item_picurl"),
                                         'AAAAAAAAB'=> array("item_collector_count","item_ends", "item_picurl"));
$result = $ob->mget($items);
var_dump($result);

unset($ob);	
?>
