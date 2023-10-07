from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from delta import DeltaTable
from pyspark.sql.functions import *

from cvmdatalake import conformed, creat_delta_table_if_not_exists, get_s3_path

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'lake_descriptor', 'cvm_environment'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark = glueContext.spark_session

lake_descriptor = args['lake_descriptor']
cvm_environment = args['cvm_environment']

creat_delta_table_if_not_exists(spark, conformed.Offers, lake_descriptor)

sample_offers = [{"directory_name": "Offer460", "offer_id": "217590109",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=217590109", "brand": "VOX Cinemas",
                  "offer_title": "Mondays at VOX Cinemas",
                  "offer_subtitle": "2X bonus points on all tickets and snacks.",
                  "offer_description": "On Mondays, you can enjoy double the SHARE points (equivalent to 6% cashback) on all your ticket and snack purchases at VOX Cinemas.<br\/>\n<br\/>\n<b\/>Where:<\/b> All VOX Cinemas locations in the UAE.<br\/>\n<br\/>\n<b\/>How:<\/b> Activate this offer then scan your SHARE ID at the cashier.",
                  "tcs": "Offering subject to change without notice. All terms and conditions of the SHARE programme applies. All terms and conditions of VOX Cinemas apply. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "06.07.2021", "offer_expiry_date": "-",
                  "limit_per_member": "-", "locations": "All particiapting locations",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EfuzYtvYsDJOkJeCIukgodEBR2zRMRoGirXGQv-eehRPZQ?e=QZakOa",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer960\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f25a175150f1d8d7fea37\/original.jpg?1662985633",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f25a1c5b4f6757a3ab3fc\/original.jpg?1662985633"},
                 {"directory_name": "Offer465", "offer_id": "1943017262",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1943017262", "brand": "SMBU",
                  "offer_title": "Friday Meal at City Centre Deira",
                  "offer_subtitle": "20X bonus points when you dine in Food Central",
                  "offer_description": "Enjoy twenty times the SHARE points when you dine anywhere in the Food Central on Fridays. <br\/>\n<br\/>\n<b\/>Where:<\/b> City Centre Deira<br\/>\n<br\/>\n<b\/>How:<\/b> Activate this offer then scan yoru receipts or SHARE ID on the SHARE app.",
                  "tcs": "Offering subject to change without notice. All terms and conditions of the SHARE programme applies. All terms and conditions of Food Central apply. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "06.03.2021", "offer_expiry_date": "-",
                  "limit_per_member": "-", "locations": "All particiapting locations",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EVEdptZexQ1Ni8lUprO9RiwB3QgijadsYzABrAmmF5dFfw?e=yhK9Ma",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer812\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201d90c5b4f660decf938e\/original.jpg?1663049104",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201d909248f071159cea91\/original.jpg?1663049104"},
                 {"directory_name": "Offer742", "offer_id": "1902367068",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1902367068", "brand": "Little Explorers",
                  "offer_title": "Complimentary workshop at Little Explorers",
                  "offer_subtitle": "Purchase a Little Explorers Day Pass and get a complimentary workshop. ",
                  "offer_description": "Purchase a Little Explorers Day Pass and get a complimentary workshop. <br\/>\n<br\/>\n<b\/>Where:<\/b> City Centre Mirdif.<br\/>\n<br\/>\n<b\/>How:<\/b> Present your SHARE ID to avail the offer.",
                  "tcs": "Complimentary workshop is to be used on the same day. Choice of workshop: Culinary, Music, Gardening, Painting, Pottery, Arts & Crafts. Not valid on public holidays or with any other offers or promotions.",
                  "target_audience": "All SHARE Members", "offer_start_date": "25.01.2022",
                  "offer_expiry_date": "31.12.2022", "limit_per_member": "-", "locations": "City Centre Mirdif",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EXAaWLYSqbRBpxZw1-rOFDwBQQ3GCoEnXcaQ_1o2ouzXNw?e=5yeeGY",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer742\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320164275150f5229983083\/original.jpg?1663047234",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320164275150f5249982bff\/original.jpg?1663047234"},
                 {"directory_name": "Offer743", "offer_id": "1727793240",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1727793240", "brand": "iFly",
                  "offer_title": "Complimentary flight at iFly",
                  "offer_subtitle": "Purchase your First Time Flyer Experience with us and get a complimentary flight.",
                  "offer_description": "Purchase your First Time Flyer Experience with us and get a complimentary flight.<br\/>\n<br\/>\n<b\/>Where:<\/b> City Centre Mirdif.<br\/>\n<br\/>\n<b\/>How:<\/b> Present your SHARE ID to avail the offer.",
                  "tcs": "Complimentary flight is to be used at the same time as your experience. Not valid on public holidays. Not valid with any other offers or promotions.",
                  "target_audience": "All SHARE Members", "offer_start_date": "25.01.2022",
                  "offer_expiry_date": "31.12.2022", "limit_per_member": "-", "locations": "City Centre Mirdif",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EQSk2ROTmxJKotR65YD17JcBiNfwDHReO_-xt4aOgudueg?e=x8fgwv",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer743\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201bf955991e512b7e3463\/original.jpg?1663048697",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201bf8e6d415631d1ee78c\/original.jpg?1663048696"},
                 {"directory_name": "Offer819", "offer_id": "1933587104",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?activateofferid=1933587104",
                  "brand": "City Centre Deira", "offer_title": "AED 25 off Food Central on Weekends",
                  "offer_subtitle": "Spend AED 80 or more at Food Central and get AED 25 back in points.",
                  "offer_description": "Spend AED 80 or more at Food Central in City Centre Deira and get AED 25 back in SHARE points every Saturday and Sunday.<br\/> \n<br\/> \n<b\/>Where:<\/b>Food Central in City Centre Deira<br\/> \n<br\/>\n <b\/>How:<\/b> Activate this offer and scan your receipts or SHARE ID.",
                  "tcs": "Offering subject to change without notice. All terms and conditions of the SHARE programme applies. All terms and conditions of Food Central apply. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "03.12.2022", "offer_expiry_date": "-",
                  "limit_per_member": "-", "locations": "City Centre Deira",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EfoBBoaSNCFHqdoEXfE_Lq0Bpc-5j_Hgep3phZPIU8ShlA?e=mbNjHR",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer819\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201d90c5b4f660decf938e\/original.jpg?1663049104",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201d909248f071159cea91\/original.jpg?1663049104"},
                 {"directory_name": "Offer922", "offer_id": "610463391",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=610463391", "brand": "Ski Dubai",
                  "offer_title": "SHARE Mondays at Ski Dubai",
                  "offer_subtitle": "Get three hours on the slopes for the price of two or a free chairlift ride.",
                  "offer_description": "Every Monday, SHARE members get three hours on the slopes for the price of two or a free chairlift ride with every Snow Park ticket (with a minimum spend of 200 points).<br\/>\n<br\/>\n<b\/>Where<\/b>: Mall of the Emirates.<br\/>\n<br\/>\n<b\/>How<\/b>: Present your SHARE ID to avail.",
                  "tcs": "Ski Dubai offers are only available on Mondays and can be redeemed when a member pays with points or a combination of points and cash. Valid for SHARE members only i.e., the SHARE app will have to be presented at Ski Dubai. Slope access is available for experienced skiers\/snowboarders only (Intermediate Level 2). Children under two years of age are not allowed in the Snow Park. The offer cannot be used in conjunction with another offer. Offers are subject to change at any time without prior notice.\n",
                  "target_audience": "All SHARE Members", "offer_start_date": "07.04.2022", "offer_expiry_date": "-",
                  "limit_per_member": "-", "locations": "Mall of the Emirates",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/ETd3c-OPMU5FilBs4E7JyOIBl3zOtzQtcJYUugz5eXcT2g?e=Y96doN",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer1009\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f24559248f02659f30ff6\/original.jpg?1662985301",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f24554626ef157142aafd\/original.jpg?1662985301"},
                 {"directory_name": "Offer923", "offer_id": "1687743753",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1687743753", "brand": "Ski Dubai",
                  "offer_title": "20% off the full-day slope pass ",
                  "offer_subtitle": "Save 20% on the full-day slope pass and get access to the slope for an entire day.",
                  "offer_description": "Shred the slopes at Ski Dubai for an entire day with the full-day pass. Use your SHARE ID and save 20% per pass. Your ski\/snowboard equipment and rental gear are included.<br\/>\n<br\/>\n<b\/>Where<\/b>: Mall of the Emirates.<br\/>\n<br\/>\n<b\/>How<\/b>: Present your SHARE ID to avail.",
                  "tcs": "Valid for full day passes, all days of the week. Valid for SHARE members only i.e., the SHARE app will have to be presented at Ski Dubai. Available at Ski Dubai only (not available on skidxb.com). Slope access is available for experienced skiers\/snowboarders only (Intermediate Level 2). For safety reasons, children under three years of age are not allowed on the slopes. Maximum of two tickets can be discounted per transaction for every SHARE member.  All slope users must sign a waiver form at the counter during the visit. It is mandatory for a parent or a guardian to sign the waiver form on behalf of all the guests under 21 years of age. Not valid on public holidays. The offer cannot be used in conjunction with another offer. Offers are subject to change at any time without prior notice.\n",
                  "target_audience": "All SHARE Members", "offer_start_date": '', "offer_expiry_date": "-",
                  "limit_per_member": "-", "locations": "Mall of the Emirates",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EZxBnUgKy2dIocfCbwdx9OgBEtOBdcLAEASSsNFvg8iIKA?e=twKg0w",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer458\/home.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f24559248f02659f30ff6\/original.jpg?1662985301",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f24554626ef157142aafd\/original.jpg?1662985301"},
                 {"directory_name": "offer973", "offer_id": "1033909815",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1033909815", "brand": "Bistro Domino",
                  "offer_title": "Dish of the Day at Bistro Domino",
                  "offer_subtitle": "Enjoy Dish of the Day? for AED 55 at Bistro Domino, ibis Dubai Deira City Centre.",
                  "offer_description": "Dine at \"Bistro Domino\" offering a selection of international favorites in a contemporary setting. The restaurant provides indoor seating and alfresco dining and is the ideal place to grab a snack, lunch\/dinner with friends and colleagues. Choose between a salad, main-course or a sandwich from ?Dish of the Day? menu for just AED 55.<br\/>\n<br\/>\n<b\/>Where:<\/b> Bistro Domino, ibis Dubai Deira City Centre.<br\/>\n<br\/>\n<b\/>How:<\/b>WhatsApp 0507335094\/email at H6481@accor.com. Daily from 12pm to 11pm",
                  "tcs": "The offer cannot be used in conjunction to any other promotion or offer. Black-out dates apply. The offer is not applicable during special dates such as Valentine?s Day, Christmas, New Year?s Eve.\n",
                  "target_audience": "All SHARE Members", "offer_start_date": "06.09.2022",
                  "offer_expiry_date": "31.12.2022", "limit_per_member": "-",
                  "locations": "ibis Dubai Deira City Centre",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EdmKZCJWDuNCqjXb5KfX_JEBC8jee8xqQE-nb-05rB_gSQ?e=UoOe8K",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer981\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201d9175150f52299852c5\/original.jpg?1663049105",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201d9075150f52299852c4\/original.jpg?1663049104"},
                 {"directory_name": "offer999", "offer_id": "1861889664",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1861889664", "brand": "Little Explorers",
                  "offer_title": "Mondays at Little Explorers",
                  "offer_subtitle": "Purchase a Little Explorers Day Pass and get a complimentary workshop and free Soft Play at Magic Planet ",
                  "offer_description": "Purchase a Little Explorers Day Pass and get a complimentary workshop and free Soft Play at Magic Planet.<br\/>\n<br\/>\n<\/b>Where<\/b>: City Centre Mirdif.<br\/>\n<br\/>\n<b\/>How<\/b>: Present your SHARE ID to avail.",
                  "tcs": "Offer only valid on Mondays. Complimentary workshop is to be used on the same day. Choice of workshop: Culinary, Music, Gardening, Painting, Pottery, Arts & Crafts. Free Soft Play at Magic Planet will be given in the form of a Magic Planet Card. Not valid on public holidays or with any other offers or promotions.",
                  "target_audience": "All SHARE Members", "offer_start_date": "30.06.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-", "locations": "City centre Mirdif",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EWu5ULb6IwZMil-uOu-usoABYEzWaowlVifCVjDcZn0cfA?e=19mW99",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer999\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320164275150f5229983083\/original.jpg?1663047234",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320164275150f5249982bff\/original.jpg?1663047234"},
                 {"directory_name": "offer1000", "offer_id": "1958110293",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?activateofferid=1958110293",
                  "brand": "Magic Planet", "offer_title": "Mondays at Magic Planet",
                  "offer_subtitle": "Purchase our exclusive SHARE Package and get 2X SHARE points and 2X free games",
                  "offer_description": "Purchase our exclusive SHARE Package and get double SHARE points and double blue swiper games (Total of 20 free blue swiper games). <br\/>\n<br\/>\n<br\/>Where<br\/>: All Magic Planet locations.<br\/>\n<br\/>\n<br\/>How<br\/>: Activate this offer and scan your SHARE ID to avail,",
                  "tcs": "Offer only valid on Mondays. Offer only valid on the SHARE Package for AED 210. 10 additional blue swiper games are complimentary to the exsisting package (total of 20). Not valid on public holidays or with any other offers or promotions. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "07.11.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-",
                  "locations": "All particiapting locations",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EX7jwU1Yb6RHiwz3KZJwJ6cBd-5Mm-M1FFd4FnttlvbsLw?e=CP9MGN",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer740\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f2271e6d4152094e4fe34\/original.jpg?1662984817",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f227175150f1d8d7fdfc8\/original.jpg?1662984817"},
                 {"directory_name": "offer1001", "offer_id": "1681082728",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1681082728", "brand": "Dreamscape",
                  "offer_title": "Mondays at Dreamscape",
                  "offer_subtitle": "Book two Dreamscape experiences for AED 130 ",
                  "offer_description": "Book two Dreamscape experiences for AED 130.<br\/>\n<br\/>\n<b\/>Where<\/b>: Mall of the Emirates.<br\/>\n<br\/>\n<b\/>How<\/b>: Present your SHARE ID to avail.",
                  "tcs": "Offer only valid on Mondays. Price includes VAT. Both experiences must be booked within the same day. Bookings must be made in-store and are subject to availability. Not valid on public holidays or with any other offers or promotions.\n",
                  "target_audience": "All SHARE Members", "offer_start_date": "30.06.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-", "locations": "Mall of the Emirates",
                  "image_file_name": "https:\/\/majidalfuttaim.sharepoint.com\/:i:\/s\/loyalty\/EdyWt5V-7E1JgCWLSefUI_kB7ZXoKXrZW0rcYoD8eKcOCw?e=Ct4mqa",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer744\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320155d55991e50b87e3fc6\/original.jpg?1663047005",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320155cc5b4f66066cf81fa\/original.jpg?1663047004"},
                 {"directory_name": "offer1004", "offer_id": "1044624142",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?activateofferid=1044624142", "brand": "Gulf News",
                  "offer_title": "15% back in SHARE points on Gulf News digital subscriptions",
                  "offer_subtitle": "15% back in SHARE points on Gulf News digital subscriptions",
                  "offer_description": "Get the latest news conveniently delivered to your phone any time and any way you like. Gulf New is celebrating their Anniversary with digital subscriptions as low as AED 9 monthly. As a SHARE member, you get an additional 15% back in SHARE points meaning that you pay as little as AED 7 on a digital newspaper.\n<br\/>\n<br\/>\n<b\/>Where<\/b>:Click the link below.<br\/>\n<br\/>\n<b\/>How<\/b>: Simply sign up for Gulf News using the link below and be sure to enter the same email address that you use to log into SHARE.",
                  "tcs": "Offering subject to change without notice. All terms and conditions of the SHARE programme applies. All terms and conditions of Gulf News apply. Offer is valid once per member.",
                  "target_audience": "All SHARE Members", "offer_start_date": "07.07.2022", "offer_expiry_date": '',
                  "limit_per_member": "-", "locations": "All particiapting locations",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EQiiOoX7BcNAukzd8ezBYZwBnc7NRzaPj7aUlZQMgNtETA?e=jmDm5l",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer1004\/home.png",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f1b5ee6d41511d9e4ec5c\/original.jpg?1662983006",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f1b5e4626ef6730429d9c\/original.jpg?1662983006"},
                 {"directory_name": "offer1005", "offer_id": "812703833",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=812703833", "brand": "Magic Planet",
                  "offer_title": "Free Bonus Credit & Games at Magic Planet",
                  "offer_subtitle": "Enjoy an exclusive SHARE Package with free bonus credit and games",
                  "offer_description": "Purchase our exclusive SHARE Monday Package for AED210 and get AED100 in bonus credit and 10 free blue swipers games.\n<br\/>\n<br\/>\n<b\/>Where<\/b>: All Magic Planet locations.<br\/>\n<br\/>\n<b\/>How<\/b>: Present your SHARE ID to avail.",
                  "tcs": "Bonus credit gets used first and is valid for 1 month from activation of the package. Not valid on public holidays or with any other offers or promotions. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "30.06.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-",
                  "locations": "All particiapting locations",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EX7jwU1Yb6RHiwz3KZJwJ6cBd-5Mm-M1FFd4FnttlvbsLw?e=CP9MGN",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer740\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f2271e6d4152094e4fe34\/original.jpg?1662984817",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f227175150f1d8d7fdfc8\/original.jpg?1662984817"},
                 {"directory_name": "offer1006", "offer_id": "1055932994",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?activateofferid=1055932994",
                  "brand": "Yalla Bowling", "offer_title": "2X at Yalla! Bowling ",
                  "offer_subtitle": "2X SHARE points at Yalla! Bowling ",
                  "offer_description": "2X SHARE points at Yalla! Bowling.<br\/>\n<br\/>\n<b\/>Where<\/b>: Mall of the Emirates, City Centre Mirdif and City Centre Al Zahia.\n<b\/>How<\/b>: Activate this offer and scan your SHARE ID to avail.",
                  "tcs": "Double points valid on game purchases only. Not valid with any other offers or promotions. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "30.06.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-",
                  "locations": "All particiapting locations",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EVf0OMRLPgxBjb5-xmkT7XsBni-5H2QugQzgWHxkm6SSVg?e=InLMxn",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer1006\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aae6d4152ea9e4f558\/original.jpg?1662985130",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa75150f16a17fed52\/original.jpg?1662985130"},
                 {"directory_name": "offer1007", "offer_id": "1747040767",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1747040767", "brand": "Dreamscape",
                  "offer_title": "Complimentary Dreamscape Adventure",
                  "offer_subtitle": "Enjoy a complimentary Dreamscape adventure when booking a private pod of 6",
                  "offer_description": "Enjoy a complimentary Dreamscape adventure when booking a private pod experience. Purchase 5 tickets and receive the 6th ticket complimentary. <br\/>\n<br\/>\n<b\/>Where<\/b>: Mall of the Emirates.<br\/>\n<br\/>\n<b\/>How<\/b>: Present your SHARE ID to avail.",
                  "tcs": "Complimentary ticket is valid when booking 5 tickets to the same experience. Valid on in-store bookings only.  Not valid on public holidays or with any other offers or promotions. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "30.06.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-", "locations": "Mall of the Emirates",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/ESmFP-fkJQ5OlOLf9FIPNiwBX2ZpNSaRPI0oH4_lwWUXgA?e=rMiTBO",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer748\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320155d55991e50b87e3fc6\/original.jpg?1663047005",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/6320155cc5b4f66066cf81fa\/original.jpg?1663047004"},
                 {"directory_name": "offer1025", "offer_id": "108738429",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=108738429", "brand": "iFly",
                  "offer_title": "Mondays at iFly",
                  "offer_subtitle": "Enjoy complimentary upgrades on all packages at iFly. ",
                  "offer_description": "Purchase our First Time Flyer package and get a complimentary upgrade to a Ultimate Flyer Package. Purchase our Ultimate Flyer package and get a complimentary upgrade to our VR package. Purhcase our VR package and get a complimentary photo package.  <br\/>\n<br\/>\n<b\/>Where:<\/b> City Centre Mirdif.<br\/>\n<br\/>\n<b\/>How:<\/b> Present your SHARE ID to avail.",
                  "tcs": "Offer only valid on Mondays. Not valid on public holidays or with any other offers or promotions.",
                  "target_audience": "All SHARE Members", "offer_start_date": "30.06.2022",
                  "offer_expiry_date": "30.12.2022", "limit_per_member": "-", "locations": "City Centre Mirdif",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/EU2tToeNZQhJvRAcC7BSNHUB9y9AA7lks3t1cfV9js7FYA?e=jJTfhc",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer747\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201bf955991e512b7e3463\/original.jpg?1663048697",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/63201bf8e6d415631d1ee78c\/original.jpg?1663048696"},
                 {"directory_name": "offer1032", "offer_id": "558265820",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=558265820",
                  "brand": "Mall of the Emirates", "offer_title": "Drop & Shop with Zeman Awwal",
                  "offer_subtitle": "Get 1 hour free on every purchase of an hour.",
                  "offer_description": "Drop the kids at Zeman Awwal and enjoy shopping at Mall of the Emirates. Yehhal Zeman Awwal includes arts and crafts that will enrich your little ones' Emarati knowledge and culture. Get 1 hour free on every purchase of an hour. Prices range from AED 70 - 100. Suitable for kids aged 3 - 9. <br\/>\n<br\/>\n<b\/>Where:<\/b> Level 2 Fashion Dome, next to Armani Cafe in the Mall of the Emirates.<br\/>\n<br\/>\n<b\/>How:<\/b> Present your SHARE ID to avail.",
                  "tcs": "Offering subject to change without notice. All terms and conditions of the SHARE programme applies. All terms and conditions of Mall of the Emirates apply. ",
                  "target_audience": "All SHARE Members", "offer_start_date": "07.07.2022", "offer_expiry_date": '',
                  "limit_per_member": "-", "locations": "Mall of the Emirates",
                  "image_file_name": "https:\/\/majidalfuttaim-my.sharepoint.com\/:i:\/g\/personal\/joanne_boujawad_maf_ae\/ERmXrvi667NOmENMVuhevp8BuCAUkn66cXnc8j6rJI1VTw?e=esfQ9F",
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer1032\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa75150f1d6c7fe2a5\/original.jpg?1662985130",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa75150f22ee7fe01b\/original.jpg?1662985130"},
                 {"directory_name": "offer1058", "offer_id": "499588141",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=499588141", "brand": "SHAREPay",
                  "offer_title": "Double the Chance to be a Millionaire",
                  "offer_subtitle": "Double entry into the SHARE Millionaire draw using SHAREPay",
                  "offer_description": "Increase your chances of winning 1 million points (AED100,000 in points) by using SHAREPay.<br\/> \n<br\/> \n<b\/>What:<\/b> Double entry into the SHARE Millionaire draw where one member wins 1 million points each week.<br\/> \n<br\/>\n <b\/>Where:<\/b> Mall of the Emirates, City Centre Mirdif, City Centre Deira and City Centre Me'aisem.<br\/> \n<br\/>\n <b\/>How:<\/b> Spend AED 300 for all your combined shopping in the mall. At each store\/restaurant, make sure you're paying with SHAREPay.",
                  "tcs": "All offerings, discounts and contests are subject to change without prior notice. This Draw is the same one as the Dubai Summer Surprises Draw and uses the same terms and conditions.  SHARE programme terms and conditions apply. All associated brand\/partner terms and conditions also apply.",
                  "target_audience": '', "offer_start_date": '', "offer_expiry_date": '',
                  "limit_per_member": '', "locations": '', "image_file_name": '',
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer1058\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa9248f0283af30bb2\/original.jpg?1662985130",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa4626ef1b1b429d30\/original.jpg?1662985130"},
                 {"directory_name": "offer1059", "offer_id": "1027094795",
                  "cta_link": "https:\/\/www.sharerewards.com\/offer?offerid=1027094795", "brand": "SHAREPay",
                  "offer_title": "SHAREPay Gives You 10X More",
                  "offer_subtitle": "10X bonus points that you can use anywhere in the mall",
                  "offer_description": "Use the award-winning SHAREPay feature this month anywhere in our malls and enjoy this special 10X bonus points.<br\/> \n<br\/> \n<b\/>What:<\/b> 10X bonus points that can be used for all your shopping and dining on your next transacton, capped at AED500.<br\/> \n<br\/>\n <b\/>Where:<\/b> Any store or restaurant in Mall of the Emirates and all City Centre locations.<br\/> \n<br\/>\n <b\/>How:<\/b> Activate this offer below then use SHAREPay when your making a payment at any store.",
                  "tcs": "All offerings, discounts and contests are subject to change without prior notice. SHARE programme terms and conditions apply. The offer is a excluding luxury category. This offer can be used one time only. This offer is capped at 5,000 bonus points. All associated brand\/partner terms and conditions also apply.",
                  "target_audience": '', "offer_start_date": '', "offer_expiry_date": '',
                  "limit_per_member": '', "locations": '', "image_file_name": '',
                  "home_large_image_thumbnail": '',
                  "offer_image": "https:\/\/dq089lg8thlr7.cloudfront.net\/offer-images\/Prod\/offer1058\/450.jpg",
                  "header_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa75150f1d6c7fe2a5\/original.jpg?1662985130",
                  "main_image": "https:\/\/cdn.braze.eu\/appboy\/communication\/assets\/image_assets\/images\/631f23aa75150f22ee7fe01b\/original.jpg?1662985130"}]

sample_offers_df: DataFrame = spark.createDataFrame(sample_offers)

offers_path = get_s3_path(conformed.Offers, lake_descriptor)
offers = DeltaTable.forPath(spark, offers_path)
offers.alias(conformed.Offers.table_name()).merge(
    sample_offers_df.alias('sample'),
    f"""
    sample.{conformed.Offers.offer_id.name} = {conformed.Offers.offer_id.column_alias()}     
    """
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

job.commit()
