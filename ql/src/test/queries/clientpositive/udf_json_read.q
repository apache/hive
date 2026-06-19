DESCRIBE FUNCTION java_read;
DESCRIBE FUNCTION EXTENDED java_read;


select json_read('[{"name":"john","alias":"j","address":{"city":"LA"}},{"name":"kinga","alias":"binga","age":2}]',
		'array<struct<name:string,age:int,alias:string,address:struct<city:string,street:string>>>');

create table t (info array<struct<name:string,age:int,alias:string,address:struct<city:string,street:string>>>);

insert into t
	select json_read('[{"name":"john","alias":"j","address":{"city":"LA"}},{"name":"kinga","alias":"binga","age":2}]',
			'array<struct<name:string,age:int,alias:string,address:struct<city:string,street:string>>>');



select json_read('[
{
    "business_id": "vcNAWiLM4dR7D2nwwJ7nCA",
    "hours": {
        "Tuesday": {
            "close": "17:00",
            "open": "08:00"
        },
        "Friday": {
            "close": "17:00",
            "open": "08:00"
        }
    },
    "open": true,
    "categories": [
        "Doctors",
        "Health & Medical"
    ],
    "review_count": 9,
    "name": "Eric Goldberg, MD",
    "neighborhoods": [],
    "attributes": {
        "By Appointment Only": true,
        "Accepts Credit Cards": true,
        "Good For Groups": 1
    },
    "type": "business"
}
]','array<struct<attributes:struct<accepts credit cards:boolean,by appointment only:boolean,good for groups:int>,business_id:string,categories:array<string>,hours:map<string,struct<close:string,open:string>>,name:string,neighborhoods:array<string>,open:boolean,review_count:int,type:string>>');
