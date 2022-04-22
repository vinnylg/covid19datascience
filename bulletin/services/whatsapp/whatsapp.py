import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from os.path import join
from bulletin import root

logger = logging.getLogger(__name__)

class WhatsApp:
    config = {
        'chromedriver_path': join(root, "services", "chromedriver.exe"),
        'get_msg_interval': 5,  # Time (seconds). Recommended value: 5
        'colors': True,  # True/False. True prints colorful msgs in console
        'ww_url': "https://web.whatsapp.com/"
    }

    def __init__(self, receiver):
        # Use a data directory so that we can persist cookies per session and not have to
        # authorize this application every time.
        # NOTE: This gets created in your home directory and can get quite large over time.
        # To fix this, simply delete this directory and re-authorize your WhatsApp Web session.
        chrome_data_dir_directory = "{0}/chrome/data_dir/whatsapp_web_cli".format(os.environ['USERPROFILE'])
        if not os.path.exists(chrome_data_dir_directory):
            os.makedirs(chrome_data_dir_directory)

        driver_options = webdriver.ChromeOptions()
        driver_options.add_argument("user-data-dir={0}".format(chrome_data_dir_directory))

        # add proxy capability
        PROXY = "scheme://terc.vinicius:123Mudar321@10.15.54.51:8080"
        driver_options.add_argument('--proxy-server=%s' % PROXY)
                            
        # setting up Chrome with selenium
        self.driver = webdriver.Chrome(executable_path=self.config['chromedriver_path'], options=driver_options)

        # open WW in browser
        self.driver.get(self.config['ww_url'])

        assert "WhatsApp" in self.driver.title

        if not receiver is None:
            self.chooseReceiver(receiver)


    def sendMsg(self, msg):
        """
        Type 'msg' in 'driver' and press RETURN
        """
        # select correct input box to type msg
        input_box = self.driver.find_element(By.XPATH, '//*[@id="main"]//footer//div[contains(@contenteditable, "true")]')
        # input_box.clear()
        input_box.click()

        action = ActionChains(self.driver)
        action.send_keys(msg)
        action.send_keys(Keys.RETURN)
        action.perform()

    def chooseReceiver(self, receiver):
        loaded = False
        while not loaded:
            try:
                # search name of friend/group
                friend_name = receiver
                input_box = self.driver.find_element(By.XPATH, '//*[@id="side"]//div[contains(@class,"copyable-text selectable-text")]')
                input_box.clear()
                input_box.click()
                input_box.send_keys(friend_name)
                input_box.send_keys(Keys.RETURN)
                loaded = True
            except:
                time.sleep(1)



class WhatsAppHandler(logging.StreamHandler):
    def __init__(self,receiver):
        logging.StreamHandler.__init__(self)
        self.whats = WhatsApp(receiver)

    def emit(self, record):
        msg = self.format(record)
        self.whats.sendMsg(msg)