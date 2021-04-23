import pymongo
import json
import multiprocessing
import requests
from bs4 import BeautifulSoup
from bson.json_util import dumps

username = ''
password = ''
PROXY_RACK_DNS = "megaproxy.rotating.proxyrack.net:222"
proxy = {"http": "http://{}:{}@{}".format(username, password, PROXY_RACK_DNS)}
# proxy = f'http://{username}:{password}@gate.smartproxy.com:7000' smartproxy method
my_client = pymongo.MongoClient("mongodb://localhost:27017/")
my_db = my_client["Noon"]
constant = "https://www.noon.com/uae-en/"
constant2 = "?limit=150"

tablets_list = ["electronics-and-mobiles/computers-and-accessories/tablets"]
phones_list = ["electronics-and-mobiles/mobiles-and-accessories/mobiles-20905"]
laptops_list = ["electronics-and-mobiles/computers-and-accessories/laptops"]
desktops_list = ["electronics-and-mobiles/computers-and-accessories/desktops"]
networking_list = ["electronics-and-mobiles/computers-and-accessories/networking-products-16523"]
flashdrives_list = ["electronics-and-mobiles/computers-and-accessories/data-storage/usb-flash-drives"]
memorycards_list = ["electronics-and-mobiles/computers-and-accessories/data-storage/memory-cards"]
nwstrg_list = ["electronics-and-mobiles/computers-and-accessories/data-storage/network-attached-storage-18079"]
ssds_list = ["electronics-and-mobiles/computers-and-accessories/data-storage/external-hard-drives?q=ssd&limit=150"]  # d
exthds_list = ["electronics-and-mobiles/computers-and-accessories/data-storage/external-hard-drives"]
hds_list = ["electronics-and-mobiles/computers-and-accessories/data-storage/internal-hard-drives"]
consoles_list = ["electronics-and-mobiles/video-games-10181/gaming-console"]
controllers_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/controllers-and-joysticks"]
pc_peripherals_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/gaming-keyboard-and-mice"]
motionsense_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/motion-sensing"]
headsets_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/microphone-and-headsets"]
consoles_accessories_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/console-accessories"]
digital_cards_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/digital-cards"]
gaming_chairs_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/gaming-chairs"]
controller_cover_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/controller-cover"]
console_stickers_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/console-sticker"]
chargers_cables_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/chargers-cables"]
stands_list = ["electronics-and-mobiles/video-games-10181/gaming-accessories/stands"]
games_list = ["electronics-and-mobiles/video-games-10181/games-34004"]
tvs_list = ["electronics-and-mobiles/television-and-video/televisions"]
home_theaters_list = ["electronics-and-mobiles/television-and-video/home-theater-systems-19095"]
projectors_list = ["electronics-and-mobiles/television-and-video/projectors"]
connectors_adapters_list = ["electronics-and-mobiles/accessories-and-supplies/audio-and-video-accessories-16161/connectors-and-adapters?sort[by]=price&sort[dir]=desc&limit=150",
                            "electronics-and-mobiles/accessories-and-supplies/audio-and-video-accessories-16161/connectors-and-adapters?sort[by]=price&sort[dir]=asc&limit=150",
                            "electronics-and-mobiles/accessories-and-supplies/audio-and-video-accessories-16161/cables-and-interconnects-16938?limit=150"]
headphone_accessories_list = ["electronics-and-mobiles/accessories-and-supplies/audio-and-video-accessories-16161/headphone-accessories"]
speakers_list = ["all-speakers"]
radios_boomboxes_list = ["electronics-and-mobiles/home-audio/radios-and-boomboxes"]
dj_karaoke_list = ["electronics-and-mobiles/home-audio/dj-electronic-music-and-karaoke"]
mp3_mp4_list = ["electronics-and-mobiles/portable-audio-and-video/mp3-and-mp4-players"]

products_dict0 = {"Tablets": tablets_list, "Phones": phones_list, "Laptops": laptops_list, "Desktops": desktops_list,
                 "Networking": networking_list, "Flash Drives": flashdrives_list, "Memory Cards": memorycards_list,
                 "Network Attached Storage": nwstrg_list, "Ssds": ssds_list, "External Hard Drives": exthds_list,
                 "Internal Hard Drives": hds_list, "Gaming_Consoles": consoles_list, "Controllers": controllers_list,
                 "Keyboard_Mice": pc_peripherals_list, "Motion_Sense": motionsense_list, "Headsets_Mics": headsets_list,
                 "Console_Accessories": consoles_accessories_list, "Digital_Cards": digital_cards_list,
                 "Gaming_Chairs": gaming_chairs_list, "Controller_Covers": controller_cover_list,
                 "Console_Stickers": console_stickers_list, "Chargers&Cables": chargers_cables_list, "Stands": stands_list,
                 "Games": games_list, "Televisions": tvs_list, "Home_Theaters": home_theaters_list,
                 "Projectors": projectors_list, "Connectors&Adapters": connectors_adapters_list,
                 "Headphone_Accessories": headphone_accessories_list, "Speakers": speakers_list, "Radios&Boobmoxes": radios_boomboxes_list,
                 "Dj&Karaoke": dj_karaoke_list, "MP3&MP4s": mp3_mp4_list}

coffee_makers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/coffee-makers"]
fryers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/fryers"]
microwaves_list = ["home-and-kitchen/home-appliances-31235/small-appliances/microwave-ovens"]
mixers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/mixers-18509"]
blenders_list = ["home-and-kitchen/home-appliances-31235/small-appliances/blenders"]
wafflers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/waffle-irons"]
juicers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/juicers"]
electric_kettles_list = ["uae-en/home-and-kitchen/home-appliances-31235/small-appliances/kettles"]
electric_cookers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/electric-cookers"]
food_processors = ["home-and-kitchen/home-appliances-31235/small-appliances/food-processors"]
fridges_list = ["home-and-kitchen/home-appliances-31235/large-appliances/refrigerators-and-freezers"]
cooking_ranges_list = ["home-and-kitchen/home-appliances-31235/large-appliances/ranges"]
water_dispensers_list = ["home-and-kitchen/home-appliances-31235/large-appliances/water-coolers-and-filters"]
food_grinders = ["home-and-kitchen/home-appliances-31235/small-appliances/specialty-appliances/food-grinders-and-mills"]
sandwich_makers_list = ["home-and-kitchen/home-appliances-31235/small-appliances/specialty-appliances/sandwich-makers-and-panini-presses"]
vacuums_list = ["home-and-kitchen/home-appliances-31235/large-appliances/vacuums-and-floor-care"]
irons_list = ["home-and-kitchen/home-appliances-31235/small-appliances/irons-and-steamers"]
heaters_list = ["home-and-kitchen/home-appliances-31235/large-appliances/heating-cooling-and-air-quality/heaters"]
air_conditioners = ["home-and-kitchen/home-appliances-31235/large-appliances/heating-cooling-and-air-quality/air-conditioners"]
air_purifiers_list = ["home-and-kitchen/home-appliances-31235/large-appliances/heating-cooling-and-air-quality/air-purifiers"]
air_humidifiers_list = ["home-and-kitchen/home-appliances-31235/large-appliances/heating-cooling-and-air-quality/humidifiers-22386"]
sewing_machines_list = ["home-and-kitchen/home-appliances-31235/small-appliances/sewing-machines"]
fans_list = ["home-and-kitchen/home-appliances-31235/large-appliances/heating-cooling-and-air-quality/household-fans"]

products_dict1 = {"Coffee_Makers": coffee_makers_list, "Fryers": fryers_list, "Microwaves": microwaves_list, "Mixers": mixers_list,
                  "Blenders": blenders_list, "Wafflers": wafflers_list, "Juicers": juicers_list, "Kettles": electric_kettles_list,
                  "Electric_Cookers": electric_cookers_list, "Food_Processors": food_processors, "Fridges": fridges_list,
                  "Cooking_Ranges": cooking_ranges_list, "Water_Dispensers": water_dispensers_list, "Food_Grinders": food_grinders,
                  "Sandwich_Maers": sandwich_makers_list, "Vacuums": vacuums_list, "Irons": irons_list, "Heaters": heaters_list,
                  "Air_Conditioners": air_conditioners, "Air_Purifiers": air_purifiers_list, "Humidifiers": air_humidifiers_list,
                  "Sewing_Machines": sewing_machines_list, "Fans": fans_list}

smartwatches_list = ["electronics-and-mobiles/wearable-technology/smart-watches-and-accessories/smartwatches"]
fitness_trackers_list = ["electronics-and-mobiles/wearable-technology/fitness-trackers-and-accessories/fitness-trackers"]
vr_headsets_list = ["electronics-and-mobiles/wearable-technology/virtual-reality-headsets"]
smartwatch_chargers_list = ["electronics-and-mobiles/wearable-technology/smart-watches-and-accessories/smartwatch-accessories/smartwatch-charger/wearables-acc-EL_01"]
smartwatch_bands_lists = ["electronics-and-mobiles/wearable-technology/smart-watches-and-accessories/smartwatch-accessories/smartwatch-band/wearables-acc-EL_01?sort[by]=price&sort[dir]=desc&limit=150",
                          "electronics-and-mobiles/wearable-technology/smart-watches-and-accessories/smartwatch-accessories/smartwatch-band/wearables-acc-EL_01?sort[by]=price&sort[dir]=asc&limit=150"]
smartwatch_protector_screens_list = ["electronics-and-mobiles/wearable-technology/smart-watches-and-accessories/smartwatch-accessories/smartwatch-screen-protectors/wearables-acc-EL_01"]
smartwatch_cases_list = ["electronics-and-mobiles/wearable-technology/smart-watches-and-accessories/smartwatch-accessories/smartwatch-cases/wearables-acc-EL_01"]
fittrack_bands_list = ["electronics-and-mobiles/wearable-technology/fitness-trackers-and-accessories/fitness-tracker-accessories/fitness-tracker-bands/wearables-acc-EL_01"]
fittrack_protector_screens_list = ["electronics-and-mobiles/wearable-technology/fitness-trackers-and-accessories/fitness-tracker-accessories/fitness-tracker-screen-protectors/wearables-acc-EL_01"]
fittrack_chragers_list = ["electronics-and-mobiles/wearable-technology/fitness-trackers-and-accessories/fitness-tracker-accessories/fitness-tracker-chargers/wearables-acc-EL_01"]
fittrack_cases_list = ["electronics-and-mobiles/wearable-technology/fitness-trackers-and-accessories/fitness-tracker-accessories/fitness-tracker-cases/wearables-acc-EL_01"]

products_dict2 = {"Smartwatches": smartwatches_list, "Fitness Trackers": fitness_trackers_list, "VR_Headsets": vr_headsets_list,
                  "Smartwatch_Chargers": smartwatch_chargers_list, "Smartwatch_Bands": smartwatch_bands_lists,
                  "Smartwatch_Screen_Protectors": smartwatch_protector_screens_list, "Smartwatch_Cases": smartwatch_cases_list,
                  "Fitness_Tracker_Bands": fittrack_bands_list, "Fitness_Tracker_Screen_Protectors": fittrack_protector_screens_list,
                  "Fitness_Tracker_Chargers": fittrack_chragers_list, "Fitness_Tracker_Cases": fitness_trackers_list}

surv_cameras_list = ["electronics-and-mobiles/camera-and-photo-16165/surveillance-cameras-18886"]
sports_cameras_list = ["electronics-and-mobiles/camera-and-photo-16165/video-17975/sports-and-action-cameras"]
mirrorless_cameras_list = ["electronics-and-mobiles/camera-and-photo-16165/digital-cameras/mirrorless-cameras"]
instant_cameras_list = ["electronics-and-mobiles/camera-and-photo-16165/instant-cameras"]
psd_cameras_list = ["electronics-and-mobiles/camera-and-photo-16165/digital-cameras/point-and-shoot-digital-cameras"]
digital_slr_cameras = ["electronics-and-mobiles/camera-and-photo-16165/digital-cameras/digital-slr-cameras"]
camera_bgs_list = ["uae-en/camera-background"]
camera_lighting_list = ["camera-lighting"]
flashes_list = ["electronics-and-mobiles/camera-and-photo-16165/flashes"]
softboxes_list = ["camera-softboxes"]
lenses_list = ["electronics-and-mobiles/camera-and-photo-16165/lenses-16166"]
tripods_monopods_list = ["electronics-and-mobiles/camera-and-photo-16165/accessories-16794/tripods-and-monopods"]
cam_cards_list = ["electronics-and-mobiles/mobiles-and-accessories/accessories-16176/camera-memory-cards"]
cam_bags_list = ["electronics-and-mobiles/camera-and-photo-16165/bags-and-cases-19385"]
cam_filters_list = ["electronics-and-mobiles/camera-and-photo-16165/accessories-16794/filters-and-accessories"]
binoculars_scopes_list = ["electronics-and-mobiles/camera-and-photo-16165/binoculars-and-scopes"]
camcorders_list = ["electronics-and-mobiles/camera-and-photo-16165/camcorders"]
films_list = ["electronics-and-mobiles/camera-and-photo-16165/accessories-16794/film"]
cam_batteries_list = ["electronics-and-mobiles/camera-and-photo-16165/accessories-16794/batteries-and-chargers-17570"]
drone_cameras_list = ["electronics-and-mobiles/camera-and-photo-16165/video-17975/quadcopters-and-accessories"]

products_dict3 = {"Surveillance_Cameras": surv_cameras_list, "Sports_Cameras": sports_cameras_list, "Mirrorless Cameras": mirrorless_cameras_list,
                  "Instant_Cameras": instant_cameras_list, "Point&Shoot_Digital_Cameras": psd_cameras_list,
                  "Digital_SLR_Cameras": digital_slr_cameras, "Camera_Backgrounds": camera_bgs_list, "Camera_Lighting": camera_lighting_list,
                  "Flashes": flashes_list, "Camera_Soft_Boxes": softboxes_list, "Lenses": lenses_list, "Tripods&Monopods": tripods_monopods_list,
                  "Memory_Cards_for_Cameras": cam_cards_list, "Camera_Bags&Cases": cam_bags_list, "Camera_Filters": cam_filters_list,
                  "Binoculars&Scopes": binoculars_scopes_list, "Camcorders": camcorders_list, "Films": films_list,
                  "Camera_Batteries&Chargers": cam_batteries_list, "Drone_Cameras&Accessories": drone_cameras_list}

kids_bedding_list = ["home-and-kitchen/bedding-16171/kids-bedding"]
sheets_pillowcases_list = ["home-and-kitchen/bedding-16171/sheets-and-pillowcases-16174"]
decorative_pillows_list = ["home-and-kitchen/bedding-16171/decorative-pillows-inserts-and-covers"]
shams_list = ["home-and-kitchen/bedding-16171/shams-bed-skirts-and-bed-frame-draperies"]
duvets_list = ["home-and-kitchen/bedding-16171/duvet-covers-and-sets"]
bed_pillows_list = ["home-and-kitchen/bedding-16171/bed-pillows"]
blankets_list = ["home-and-kitchen/bedding-16171/blankets-and-throws"]
comforters_list = ["home-and-kitchen/bedding-16171/comforters-and-sets"]
bedding_collections_list = ["home-and-kitchen/bedding-16171/bedding-collections-26143"]

products_dict4 = {"Kids_Bedding": kids_bedding_list, "Sheets&Pillowcases": sheets_pillowcases_list,
                  "Decorative_Pillows,Inserts&Covers": decorative_pillows_list, "Shams,Bed_Skirts&Bed_Frame_Draperies": shams_list,
                  "Duvet_Covers&Sets": duvets_list, "Bed_Pillows": bed_pillows_list, "Blankets&Throws": blankets_list,
                  "Comforters&Sets": comforters_list, "Bedding_Collections": bedding_collections_list}

towels_list = ["home-and-kitchen/bath-16182/towels-19524"]
bath_rugs_list = ["home-and-kitchen/bath-16182/bath-rugs"]
bath_robes_list = ["home-and-kitchen/bath-16182/bath_linen/bath-robes"]
scales_list = ["home-and-kitchen/bath-16182/scales-25025"]
bathroom_accessories_list = ["home-and-kitchen/bath-16182/bathroom-accessories"]
bath_hardware_list = ["home-and-kitchen/bath-16182/bath-hardware"]
bath_organization_list = ["home-and-kitchen/bath-16182/bathroom-storage-and-organisation"]

products_dict5 = {"Towels": towels_list, "Bath_Rugs": bath_rugs_list, "Bath_Robes": bath_robes_list, "Scales": scales_list,
                  "Bathroom_Accessories": bathroom_accessories_list, "Bath_Hardware": bath_hardware_list,
                  "Bathroom_Storage&Organisation": bath_organization_list}

home_lighting_list = ["home-and-kitchen/home-decor/home-decor-lighting"]
candles_list = ["home-and-kitchen/home-decor/candles-and-holders"]
home_fragrance_list = ["home-and-kitchen/home-decor/home-fragrance"]
paintings_list = ["home-and-kitchen/home-decor/artwork/paintings-18461"]
wall_stickers_list = ["home-and-kitchen/home-decor/artwork/wall-stickers?sort[by]=price&sort[dir]=desc&limit=150",
                      "home-and-kitchen/home-decor/artwork/wall-stickers?sort[by]=price&sort[dir]=asc&limit=150"]
posters_list = ["home-and-kitchen/home-decor/artwork/posters-and-prints?sort[by]=price&sort[dir]=desc&limit=150",
                "home-and-kitchen/home-decor/artwork/posters-and-prints?sort[by]=price&sort[dir]=asc&limit=150"]
tapestries_list = ["home-and-kitchen/home-decor/tapestries"]
clocks_list = ["home-and-kitchen/home-decor/clocks-16151"]
mirrors_list = ["home-and-kitchen/home-decor/mirrors-16780"]
dec_pillows_list = ["home-and-kitchen/home-decor/decorative-pillows-16073?sort[by]=price&sort[dir]=desc&limit=150",
                    "home-and-kitchen/home-decor/decorative-pillows-16073?sort[by]=price&sort[dir]=asc&limit=150"]
chair_pads_list = ["home-and-kitchen/home-decor/slipcovers/chair-pads"]
sofa_slipcovers_list = ["home-and-kitchen/home-decor/slipcovers/sofa-slipcovers"]
cushion_covers_list = ["home-and-kitchen/home-decor/slipcovers/cushion-cover"]
slipcover_sets_list =["home-and-kitchen/home-decor/slipcovers/slipcover-sets"]
home_decor_accents = ["home-and-kitchen/home-decor/home-decor-accents?sort[by]=price&sort[dir]=desc&limit=150",
                      "home-and-kitchen/home-decor/home-decor-accents?sort[by]=price&sort[dir]=asc&limit=150"]
rugs_list = ["home-and-kitchen/home-decor/area-rugs-and-pads"]
draperies_list = ["home-and-kitchen/home-decor/window-treatments-16927/draperies-and-curtains"]

products_dict6 = {"Home_Decor_Lighting": home_lighting_list, "Candles&Holders": candles_list, "Home_Fragrance": home_fragrance_list,
                  "Paintings": paintings_list, "Wall_Stickers": wall_stickers_list, "Posters&Prints": posters_list,
                  "Tapestries": tapestries_list, "Clocks": clocks_list, "Mirrors": mirrors_list, "Decorative_Pillows": dec_pillows_list,
                  "Chair_Pads": chair_pads_list, "Sofa_Slipcovers": sofa_slipcovers_list, "Cushion_Cover": cushion_covers_list,
                  "Slipcover_Sets": slipcover_sets_list}

products_dict65 = {"Home_Decor_Accents": home_decor_accents, "Area_Rugs&Pads": rugs_list, "Draperies&Curtains": draperies_list}

cookware_list = ["home-and-kitchen/kitchen-and-dining/cookware"]
bakeware_list = ["home-and-kitchen/kitchen-and-dining/bakeware"]
small_appliances_list = ["home-and-kitchen/home-appliances-31235/small-appliances"]
serveware_list = ["home-and-kitchen/kitchen-and-dining/serveware"]
baking_supplies_list = ["baking-supplies"]
kitchen_utensils_list = ["home-and-kitchen/kitchen-and-dining/kitchen-utensils-and-gadgets?sort[by]=price&sort[dir]=desc&limit=150",
                         "home-and-kitchen/kitchen-and-dining/kitchen-utensils-and-gadgets?sort[by]=price&sort[dir]=asc&limit=150"]
kitchen_linens_list = ["home-and-kitchen/kitchen-and-dining/kitchen-and-table-linens"]
kitchen_knives_list = ["home-and-kitchen/kitchen-and-dining/kitchen-knives-and-cutlery-accessories"]
flatware_list = ["home-and-kitchen/kitchen-and-dining/flatware-16540"]
cookbooks_list = ["books/lifestyle-sport-and-leisure/cookbooks-food-and-drink/cookbooks"]

products_dict7 = {"Cookware": cookware_list, "Bakeware": bakeware_list, "Small_Appliances": small_appliances_list,
                  "Dinnerware&Serveware": serveware_list, "Baking_Supplies": baking_supplies_list, "Kitchen_Utensils&Gadgets": kitchen_utensils_list,
                  "Kitchen&Table_Linens": kitchen_linens_list, "Kitchen_Knives&Cutlery_Accessories": kitchen_knives_list,
                  "Flatware&Cutlery": flatware_list, "Cookbooks": cookbooks_list}

power_tools_list = ["tools-and-home-improvement/power-and-hand-tools/power-tools"]
hand_tools_list = ["tools-and-home-improvement/power-and-hand-tools/hand-tools-16032"]
electrical_supplies = ["tools-and-home-improvement/tools-lighting-electrical"]
painting_supplies_list = ["tools-and-home-improvement/painting-supplies-and-wall-treatments"]
safety_list = ["tools-and-home-improvement/safety-and-security"]
kitchen_fixtures_list = ["tools-and-home-improvement/kitchen-and-bath-fixtures"]
pressure_washers_list = ["tools-and-home-improvement/pressure-washers"]
hardware_list = ["tools-and-home-improvement/hardware-16055"]
gardening_list = ["home-and-kitchen/patio-lawn-and-garden/gardening-and-lawn-care"]
household_supplies = ["home-and-kitchen/household-supplies/cleaning-supplies-16799?q=household%20supplies"]  # different
furniture_list = ["home-and-kitchen/furniture-10180"]
home_storage_list = ["home-and-kitchen/storage-and-organisation?sort[by]=price&sort[dir]=desc&limit=150",
                     "home-and-kitchen/storage-and-organisation?sort[by]=price&sort[dir]=asc&limit=150"]

products_dict8 = {"Power_Tools": power_tools_list, "Hand_Tools": hand_tools_list, "Electrical&Lighting_Supplies": electrical_supplies,
                  "Painting_Supplies&Wall_Treatments": painting_supplies_list, "Safety&Security": safety_list,
                  "Kitchen&Bath_Fixtures": kitchen_fixtures_list, "Pressure_Washers": pressure_washers_list,
                  "Hardware": hardware_list, "Gardening&Lawn_Care": gardening_list, "Household_Supplies": household_supplies,
                  "Furniture": furniture_list}

baby_transport_list = ["baby-products/baby-transport"]
nursing_list = ["baby-products/feeding-16153"]
baby_care_list = ["baby-products/bathing-and-skin-care"]
diapering_list = ["baby-products/diapering"]
baby_clothing_list = ["baby-products/clothing-shoes-and-accessories"]
baby_toys_list = ["toys-and-games/baby-and-toddler-toys"]
diaper_bags_list = ["baby-products/diaper-bags-17618"]
creams_list = ["creams-oils-BA_06"]

products_dict9 = {"Baby_Transport": baby_transport_list, "Nursing&Feeding": nursing_list, "Bathing&Baby_Care": baby_care_list,
                  "Diapering": diapering_list, "Baby_Clothing&Shoes": baby_clothing_list, "Baby&Toddler_Toys": baby_toys_list,
                  "Diaper_Bags": diaper_bags_list, "Creams&Oils": creams_list}

men_fragrance_list = ["beauty-and-health/beauty/fragrance?f[fragrance_department]=men&limit=150"]
women_fragrance_list = ["beauty-and-health/beauty/fragrance?f[fragrance_department]=women&limit=150"]
unisex_fragrance_list = ["beauty-and-health/beauty/fragrance?f[fragrance_department]=unisex&limit=150"]
lipstick_list = ["beauty-and-health/beauty/makeup-16142/lips/lipstick?sort[by]=price&sort[dir]=desc&limit=150",
                 "beauty-and-health/beauty/makeup-16142/lips/lipstick?sort[by]=price&sort[dir]=asc&limit=150"]
lip_liners_list = ["beauty-and-health/beauty/makeup-16142/lips/lip-liners"]
lip_plumpers_list = ["beauty-and-health/beauty/makeup-16142/lips/lip-plumpers"]
lip_stains_list = ["beauty-and-health/beauty/makeup-16142/lips/lip-stains"]
lip_glosses_list = ["uae-en/beauty-and-health/beauty/makeup-16142/lips/lip-glosses"]
lip_palettes_list = ["beauty-and-health/beauty/makeup-16142/lips/lip-palettes"]
eyes_beauty_list = ["beauty-and-health/beauty/makeup-16142/eyes-17047"]
face_beauty_list = ["beauty-and-health/beauty/makeup-16142/face-18064"]
nails_list = ["beauty-and-health/beauty/makeup-16142/nails-20024?sort[by]=price&sort[dir]=desc&limit=150",
              "beauty-and-health/beauty/makeup-16142/nails-20024?sort[by]=price&sort[dir]=asc&limit=150"]


products_dict10 = {"Men_Fragrance": men_fragrance_list, "Women_Fragrance": women_fragrance_list, "Unisex_Fragrance": unisex_fragrance_list,
                   "Lipstick": lipstick_list, "Lip_Liners": lip_liners_list, "Lip_Plumpers": lip_plumpers_list,
                   "Lip_Stains": lip_stains_list, "Lip_Glosses": lip_glosses_list, "Lip_Palettes": lip_palettes_list,
                   "Eyes_Beauty": eyes_beauty_list, "Face_Beauty": face_beauty_list, "Nails": nails_list}

eyelash_tools_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/eyelash-tools"]
makeup_mirrors_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/makeup-mirrors"]
temp_tats_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/temporary-tattoos-21319"]
cosmetic_bags_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/cosmetic-bags"]
brush_sets_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/brushes-and-applicators-25662"]
brushes_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/brushes-and-applicators-26364"]
tweezers_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/makeup-tweezers"]
makeup_cleaners_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/makeup-cleaners"]
makeup_sponges_list = ["beauty-and-health/beauty/makeup-16142/makeup-brushes-and-tools/Makeup-sponges"]
styling_tools_list = ["uae-en/beauty-and-health/beauty/hair-care/styling-tools"]
hair_colors_list = ["beauty-and-health/beauty/hair-care/hair-color"]
styling_products_list = ["beauty-and-health/beauty/hair-care/styling-products-17991"]
scalp_treatments_list = ["beauty-and-health/beauty/hair-care/hair-and-scalp-treatments-24161"]
shampoos_list = ["beauty-and-health/beauty/hair-care/shampoo-and-conditioners?sort[by]=price&sort[dir]=desc&limit=150",
                 "beauty-and-health/beauty/hair-care/shampoo-and-conditioners?sort[by]=price&sort[dir]=asc&limit=150"]
hair_accessories_list = ["beauty-and-health/beauty/hair-care/hair-accessories/styling-accessories"]
hair_elastics_list = ["beauty-and-health/beauty/hair-care/hair-accessories/styling-accessories-elastics"]
combs_list = ["beauty-and-health/beauty/hair-care/hair-accessories/styling-accessories-comb"]
wigs_list = ["beauty-and-health/beauty/hair-care/hair-accessories/hair-extensions-and-wigs"]
hair_clips_list = ["beauty-and-health/beauty/hair-care/hair-accessories/clips-24997"]
headbands_list = ["beauty-and-health/beauty/hair-care/hair-accessories/headbands-25831"]

products_dict11 = {"Eyelash_Tools": eyelash_tools_list, "Makeup_Mirrors": makeup_mirrors_list, "Temp_Tattoos": temp_tats_list,
                   "Cosmetic_Bags": cosmetic_bags_list, "Brush_Sets": brush_sets_list, "Brushes": brushes_list,
                   "Tweezers": tweezers_list, "Makeup_Cleaners": makeup_cleaners_list, "Makeup_Sponges": makeup_sponges_list,
                   "Styling_Tools": styling_tools_list, "Hair_Color": hair_colors_list, "Styling_Products": styling_products_list,
                   "Hair&Scalp_Treatments": scalp_treatments_list, "Shampoos&Conditioners": shampoos_list,
                   "Hair_Styling_Accessories": hair_accessories_list, "Hair_Elastics": hair_elastics_list,
                   "Combs": combs_list, "Wigs": wigs_list, "Hair_Clips": hair_clips_list, "Headbands": headbands_list}

skincare_tools_list = ["beauty-and-health/beauty/skin-care-16813/tools-and-accessories"]
eye_treatments_list = ["beauty-and-health/beauty/skin-care-16813/eyes-19388"]
sun_care_list = ["beauty-and-health/beauty/skin-care-16813/sun"]
skin_cleansers_list = ["beauty-and-health/beauty/skin-care-16813/skincare-cleansers"]
skin_moisturizers_list = ["beauty-and-health/beauty/skin-care-16813/moisturizers?sort[by]=price&sort[dir]=desc&limit=150",
                          "beauty-and-health/beauty/skin-care-16813/moisturizers?sort[by]=price&sort[dir]=asc&limit=150"]
skin_treatment_list = ["beauty-and-health/beauty/skin-care-16813/treatment-and-serums?sort[by]=price&sort[dir]=desc&limit=150",
                       "beauty-and-health/beauty/skin-care-16813/treatment-and-serums?sort[by]=price&sort[dir]=asc&limit=150"]
beauty_tools = ["beauty-tools-and-accessories"]
feminine_care_list = ["beauty-and-health/beauty/personal-care-16343/feminine-care"]
# bath_body_list = ["beauty-and-health/beauty/personal-care-16343/bath-and-body?sort[by]=price&sort[dir]=desc&limit=150",
#                   "beauty-and-health/beauty/personal-care-16343/bath-and-body?sort[by]=price&sort[dir]=asc&limit=150"]
shaving_list = ["beauty-and-health/beauty/personal-care-16343/shaving-and-hair-removal"]
eye_care_list = ["beauty-and-health/beauty/personal-care-16343/eye-care?sort[by]=price&sort[dir]=desc&limit=150",
                 "beauty-and-health/beauty/personal-care-16343/eye-care?sort[by]=price&sort[dir]=asc&limit=150"]
oral_hygiene_list = ["beauty-and-health/beauty/personal-care-16343/oral-hygiene"]
hand_foot_care_list = ["beauty-and-health/beauty/personal-care-16343/foot-care-18632"]
deodorants_list = ["beauty-and-health/beauty/personal-care-16343/deodorants-and-antiperspirants"]
hand_washes_list = ["beauty-and-health/beauty/personal-care-16343/hand-washes"]
men_grooming_list = ["men-grooming"]
healthcare_list = ["beauty-and-health/health?sort[by]=price&sort[dir]=desc&limit=150",
                   "beauty-and-health/health?sort[by]=price&sort[dir]=asc&limit=150"]

products_dict12 = {"Skincare_Tools": skincare_tools_list, "Eye_Treatments": eye_treatments_list, "Sun_Care": sun_care_list,
                   "Skin_Cleansers": skin_cleansers_list, "Skin_Moisturizers": skin_moisturizers_list, "Skin_Treatments": skin_treatment_list,
                   "Beauty_Tools": beauty_tools, "Feminine_Care": feminine_care_list}

products_dict125 = {"Shaving": shaving_list, "Eye_Care": eye_care_list, "Oral_Hygiene": oral_hygiene_list,
                    "Hand&Foot_Care": hand_foot_care_list, "Deorodants": deodorants_list, "Hand_Washes": hand_washes_list,
                    "Men_Grooming": men_grooming_list}

products_dict1255 = {"Healthcare": healthcare_list, "Home_Storage&Organisation": home_storage_list}

exercise_list = ["sports-and-outdoors/exercise-and-fitness?sort[by]=price&sort[dir]=desc&limit=150",
                 "sports-and-outdoors/exercise-and-fitness?sort[by]=price&sort[dir]=asc&limit=150"]
cycling_list = ["sports-and-outdoors/cycling-16009"]
combat_sports_list = ["sports-and-outdoors/combat-sports"]
camping_list = ["sports-and-outdoors/outdoor-recreation/camping-and-hiking-16354"]
team_sports_list = ["sports-and-outdoors/team-sports"]
boating_list = ["sports-and-outdoors/boating-and-water-sports"]
racquet_list = ["sports-and-outdoors/racquet-sports-16542"]
hunting_list = ["sports-and-outdoors/hunting-and-fishing"]
skates_list = ["sports-and-outdoors/action-sports"]
equestrian_list = ["sports-and-outdoors/equestrian-sports"]
game_room_list = ["sports-and-outdoors/leisure-sports-and-games/game-room"]
sports_nutrition_list = ["sports-and-outdoors/sports-nutrition-sports"]
block_balls_list = ["sports-blocks-balls"]

products_dict13 = {"Exercise&Fitness": exercise_list, "Cycling&Endurance": cycling_list, "Combat_Sports": combat_sports_list,
                   "Camping&Hiking": camping_list, "Team_Sports": team_sports_list, "Boating&Water_Sports": boating_list,
                   "Racquet_Sports": racquet_list, "Hunting&Fishing": hunting_list, "Skates, Skateboards & Scooters": skates_list,
                   "Equestrian_Sports": equestrian_list, "Game_room": game_room_list, "Sports_Nutrition": sports_nutrition_list,
                   "Block&Balls": block_balls_list}


def open_url(url):
    global proxy
    headers = {'User-Agent': 'Mozilla/5.0'}
    return requests.get(url, proxies=proxy, headers=headers).content


def get_texts(soup):
    title = soup.find('div', class_='coreWrapper').select('h1')[0].getText()
    highlights_texts = []
    if soup.find('ul', class_='highlights') is not None:
        highlights = soup.find('ul', class_='highlights').select('li')
        for i in highlights:
            highlights_texts.append(i.getText())
    specs_texts = []
    s = soup.find('script', type='application/json')
    struct = json.loads(s.contents[0])
    specs = struct['props'].get("pageProps").get("catalog").get("product").get("specifications")
    for i in specs:
        specs_texts.append((i.get("name"), i.get("value")))
    ov_section = soup.select('.overviewSection')
    overview = ""
    if len(ov_section) > 1:
        overview = ov_section[1].getText().strip("Overview")
    return title, highlights_texts, specs_texts, overview


def scrape_product(url, col):
    req = open_url(url)
    soup = BeautifulSoup(req, 'html.parser')
    img = soup.select('.pdpImage')[0]['src']
    en_text = get_texts(soup)
    price_span = soup.find('span', class_='sellingPrice')  # gets selling price
    if price_span is None:
        value = 0
    else:
        value = price_span.select('.value')[0].getText()
    # value = soup.select('.value')[0].select('.value')[0].getText()  # gets regular price
    # arabic section
    url_ar = url.replace("/uae-en/", "/uae-ar/")
    req_ar = open_url(url_ar)
    soup_ar = BeautifulSoup(req_ar, 'html.parser')
    ar_text = get_texts(soup_ar)
    col.insert_one({"_id": url, "en_title": en_text[0], "value": value, "image": img, "en_overview": en_text[3],
                       "en_highlights": en_text[1], "en_specs": en_text[2], "ar_title": ar_text[0],
                       "ar_overview": ar_text[3], "ar_highlights": ar_text[1], "ar_specs": ar_text[2]})
    print(en_text[0] + " complete")


def loop_products(product):
    global my_db
    split_str = product.split()
    url = split_str[0]
    col = my_db[split_str[1]]
    print(split_str[1])
    # print(col)
    link = "https://www.noon.com" + url
    print(link)
    if not col.find_one({"_id": link}):
        print("scraping")
        scrape_product(link, col)


def scrap_section(url, page_num, col):
    global my_db
    req = open_url(url + "&page=" + str(page_num))
    soup = BeautifulSoup(req, 'html.parser')
    products = soup.find('div', class_='productList')
    pool = multiprocessing.Pool()
    if products and page_num < 51:
        products = products.select('.product')
        product_list = [(product['href'] + " " + col) for product in products]
        print(page_num)
        pool.map(loop_products, product_list)
        scrap_section(url, page_num + 1, col)
    else:
        print("section complete")


def scrape_everything(dic):
    global constant
    global constant2
    for i in dic:
        for a in dic[i]:
            print(a)
            url = a
            if i != "Ssds"and i != "Household_Supplies" and len(dic[i]) < 2:
                url = a + constant2
            scrap_section(constant + url, 1, i)
        print(my_db[i].count_documents({}))


def jexport():
    collection = my_db.collection_name
    # cursor = collection.find({})
    col_names = my_db.list_collection_names()
    for col in col_names:
        with open(col + '.json', 'w') as file:
            file.write('[')
            for document in my_db[col].find():
                file.write(dumps(document))
                file.write(',')
            file.write(']')


def every_scrap():
    scrape_everything(products_dict0)
    scrape_everything(products_dict1)
    scrape_everything(products_dict2)
    scrape_everything(products_dict3)
    scrape_everything(products_dict4)
    scrape_everything(products_dict5)
    scrape_everything(products_dict6)
    scrape_everything(products_dict65)
    scrape_everything(products_dict7)
    scrape_everything(products_dict8)
    scrape_everything(products_dict9)
    scrape_everything(products_dict10)
    scrape_everything(products_dict11)
    scrape_everything(products_dict12)  # REVISIT
    scrape_everything(products_dict125)
    scrape_everything(products_dict1255)  # REVISIT
    scrape_everything(products_dict13)


def printdocs(col_names):
    total = 0
    for col in col_names:
        total += my_db[col].count_documents({})
    print(str(total) + " documents")


def find_zeros(names):
    total = 0
    for col_name in names:
        total += my_db[col_name].count_documents({"value": 0})
    print(total)


if __name__ == '__main__':
    # every_scrap()
    col_names = my_db.list_collection_names()
    # print(col_names)
    print("Counting documents...")
    # printdocs(col_names)
    # jexport()
    find_zeros(col_names)
    while True:
        key = input("insert collection name: ")
        if key != 'q':
            print(my_db[key].count_documents({}))
        else:
            quit()
