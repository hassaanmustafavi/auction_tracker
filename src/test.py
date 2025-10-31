from main import test_insert_links

if __name__ == "__main__":

    test_links = [
        "https://www.auction.com/details/17029-peaceful-valley-dr-wimauma-fl-1882044",
        "https://www.auction.com/details/7160-davenport-ln-spring-hill-fl-1888836",
        "https://www.auction.com/details/3850-sw-124th-ave-miramar-fl-1868311",
        "https://www.auction.com/details/3209-nw-2nd-pl-cape-coral-fl-1873663",
        "https://www.auction.com/details/6989-calle-del-paz-w-boca-raton-fl-1842251",
        "https://www.auction.com/details/3209-nw-2nd-pl-cape-coral-fl-1873663",
        "https://www.auction.com/details/6989-calle-del-paz-w-boca-raton-fl-1842251",
        "https://www.auction.com/details/1753-open-field-loop-brandon-fl-1881948",
    ]
    test_insert_links(zone="EAST", state="test6", links=test_links)

    #test_prev_update_run(zone="Test", preview_only=True)

