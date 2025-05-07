from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from selenium import webdriver # type: ignore
from selenium.webdriver.common.by import By # type: ignore
from selenium.webdriver.chrome.service import Service # type: ignore
from selenium.webdriver.chrome.options import Options # type: ignore
from webdriver_manager.chrome import ChromeDriverManager # type: ignore
from selenium.webdriver.support.ui import WebDriverWait # type: ignore
from selenium.webdriver.support import expected_conditions as EC # type: ignore
import tempfile
import os
import time

class CrawlData:
    """
    Provide services to perform clicking, data from the website,...
    """
    @staticmethod
    def simulate_click_path(URL, path, path_type="css", wait_time=10, driver=None, headless=False, time_sleep=0.5):
        """
        Navigates to a given URL, locates an HTML element using a specified selector type and path, clicks it,
        and returns the resulting page source.

        Parameters:
        - URL (str): The web page URL to navigate to.
        - path (str): The selector path to locate the element (e.g., CSS selector, XPath, ID).
        - path_type (str): The type of selector used in 'path'. Supported values are:
            "css", "xpath", "id", "name", "tag", "class", "link_text", "partial_link_text".
            Default is "css".
        - wait_time (int): Maximum number of seconds to wait for the element to become clickable. Default is 10 seconds.
        - driver (webdriver, optional): An existing Selenium WebDriver instance. If None, a new one is created.
        - headless (bool): If True, runs the browser in headless mode (no GUI). Default is False.
        - time_sleep (float): Time in seconds to wait after clicking the element. Default is 0.5 seconds.

        Returns:
        - str or None: The HTML source of the page after the click, or None if an error occurred.

        Raises:
        - ValueError: If an unsupported path_type is provided.
        """
        if driver is None:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--no-sandbox")
            driver = webdriver.Chrome(options=chrome_options)

        # Mapping path_type to By.*
        by_map = {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "tag": By.TAG_NAME,
            "class": By.CLASS_NAME,
            "link_text": By.LINK_TEXT,
            "partial_link_text": By.PARTIAL_LINK_TEXT
        }

        by = by_map.get(path_type.lower())
        if by is None:
            raise ValueError(f"Unsupported path_type '{path_type}'. Supported types: {list(by_map.keys())}")

        page_source = None
        try:
            driver.get(URL)
            element = WebDriverWait(driver, wait_time).until(
                EC.element_to_be_clickable((by, path))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", element)
            driver.execute_script("arguments[0].click();", element)
            time.sleep(time_sleep)
            page_source = driver.page_source
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if driver is not None:
                driver.quit()

        return page_source

    @staticmethod
    def navigate_and_collect_click_data(url, selector, selector_type="css", wait_time=10, max_clicks=5, driver=None, headless=False, post_click_delay=0.5, new_data_selector=None):
        """
        Navigates to a given URL, repeatedly clicks an HTML <a> element using a specified selector,
        and collects page source after each click. Optimized for <a> tags with retry logic.

        Parameters:
        - url (str): The web page URL to navigate to.
        - selector (str): The selector path to locate the <a> element (e.g., CSS selector, XPath).
        - selector_type (str): The type of selector used in 'selector'. Supported values are:
            "css", "xpath", "id", "name", "tag", "class", "link_text", "partial_link_text".
            Default is "css".
        - wait_time (s): Maximum seconds to wait for the element to become clickable. Default is 10.
        - max_clicks (int): Maximum number of clicks to perform. Default is 5.
        - driver (webdriver, optional): An existing Selenium WebDriver instance. If None, a new one is created.
        - headless (bool): If True, runs the browser in headless mode (no GUI). Default is False.
        - post_click_delay (s): Seconds to wait after clicking the element. Default is 0.5.
        - new_data_selector (str, optional): CSS selector for an element that indicates new data has loaded.

        Returns:
        - list: List of page sources after each click, or empty list if an error occurred.

        Raises:
        - ValueError: If an unsupported selector_type is provided.
        """
        if driver is None:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--no-sandbox")
            driver = webdriver.Chrome(options=chrome_options)

        # Mapping selector_type to By.*
        by_map = {
            "css": By.CSS_SELECTOR,
            "xpath": By.XPATH,
            "id": By.ID,
            "name": By.NAME,
            "tag": By.TAG_NAME,
            "class": By.CLASS_NAME,
            "link_text": By.LINK_TEXT,
            "partial_link_text": By.PARTIAL_LINK_TEXT
        }

        by = by_map.get(selector_type.lower())
        if by is None:
            raise ValueError(f"Unsupported selector_type '{selector_type}'. Supported types: {list(by_map.keys())}")

        page_sources = []
        try:
            driver.get(url)
            # Collect initial page source
            page_source = driver.page_source
            if page_source:
                page_sources.append(page_source)
            else:
                print("Warning: Initial page source is empty or None")
                return page_sources

            for i in range(max_clicks):
                retries = 0
                max_retries = 3
                while retries < max_retries:
                    try:
                        # Wait for the <a> element to be clickable
                        element = WebDriverWait(driver, wait_time).until(
                            EC.element_to_be_clickable((by, selector))
                        )
                        # Verify it's an <a> tag
                        if element.tag_name != 'a':
                            print(f"Warning: Element at click {i+1} is not an <a> tag")
                            return page_sources
                        # Check href attribute
                        href = element.get_attribute('href')
                        if not href:
                            print(f"Warning: <a> element at click {i+1} has no href attribute")
                        # Scroll to center and wait for overlays
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                        time.sleep(0.5)  # Wait for overlays/animations
                        # Wait for any loading spinner to disappear
                        try:
                            WebDriverWait(driver, 3).until(
                                EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loading, .spinner, .overlay"))
                            )
                        except Exception:
                            pass
                        # Click using JavaScript
                        driver.execute_script("arguments[0].click();", element)
                        # Wait for page to update
                        time.sleep(post_click_delay)
                        # Wait for new data if specified
                        if new_data_selector:
                            WebDriverWait(driver, wait_time).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, new_data_selector))
                            )
                        # Collect page source
                        page_source = driver.page_source
                        if page_source:
                            page_sources.append(page_source)
                        else:
                            print(f"Warning: Page source after click {i+1} is empty or None")
                            break
                        break  # Success, exit retry loop
                    except Exception as e:
                        retries += 1
                        print(f"Error during click {i+1}, retry {retries}/{max_retries}: {e}")
                        if retries == max_retries:
                            print(f"Max retries reached for click {i+1}, stopping.")
                            return page_sources
                        time.sleep(1)  # Wait before retrying
                # Check if <a> element still exists
                elements = driver.find_elements(by, selector)
                if not elements:
                    print(f"<a> element with selector '{selector}' no longer exists after click {i+1}, stopping.")
                    break
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if driver is not None:
                driver.quit()

        return page_sources
    
    @staticmethod
    def get_attribute_value(page_source, selector, attribute="text", base_url=""):
        """
        Extracts text or attribute values from HTML elements matching a CSS selector.

        Args:
            page_source (str): HTML content as a string.
            selector (str): CSS selector to identify target elements.
            attribute (str, optional): Attribute name to extract (default is "text").
            base_url (str, optional): Base URL used to resolve relative 'href' values.

        Returns:
        soup = BeautifulSoup(page_source, HTML_PARSER)
        """
        soup = BeautifulSoup(page_source, "html.parser")
        elements = soup.select(selector)

        if not elements:
            return set()

        attr_values = set()

        for element in elements:
            if attribute.lower() == "text":
                attr_values.add(element.get_text(strip=True))
            else:
                attr_value = element.get(attribute) if element.has_attr(attribute) else None
                if attribute.lower() == "href" and attr_value:
                    parsed_url = urlparse(attr_value)
                    if not parsed_url.netloc:
                        attr_value = urljoin(base_url, attr_value)
                if attr_value:
                    attr_values.add(attr_value)

        return attr_values

    @staticmethod
    def get_attribute_value_with_selenium(url, selector, attribute="text", by=By.CSS_SELECTOR, driver=None, headless=False):
        """
        Extracts text or attribute values from web elements on a page using Selenium WebDriver.

        Parameters:
            url (str): The URL of the web page to scrape.
            selector (str): The CSS selector or XPath to identify target elements.
            attribute (str, optional): The attribute to extract (default is "text").
            by (By, optional): The method of locating elements (default is By.CSS_SELECTOR).
            driver (WebDriver, optional): An existing Selenium WebDriver instance. If None, a new one is created.
            headless (bool, optional): If True, the browser runs in headless mode (default is False).

        Returns:
            set: A set of unique values extracted from the elements (either text or attribute).
        """
        should_quit = False

        if driver is None:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            should_quit = True

        result = set()

        try:
            driver.get(url)
            elements = driver.find_elements(by, selector)
            for element in elements:
                if attribute.lower() == "text":
                    value = element.text.strip() if element.text else None
                else:
                    value = element.get_attribute(attribute) if element else None

                if value:
                    result.add(value)

        except Exception as e:
            print(f"Error: {e}")
        finally:
            if should_quit:
                driver.quit()

        return result
        
    @staticmethod
    def get_child_selectors(page_source, selector):
        """
        Extracts child element selectors from a parent element identified by a CSS selector in the given HTML.

        Parameters:
            page_source (str): The HTML content of the page as a string.
            selector (str): The CSS selector to identify the parent element.

        Returns:
            set: A set of unique CSS selectors for the child elements of the parent element.
        """
        soup = BeautifulSoup(page_source, "html.parser")
        parent = soup.select_one(selector)

        if not parent:
            return set()

        child_selectors = set()
        for child in parent.find_all(recursive=False):
            tag_name = child.name
            class_name = ".".join(child.get("class", []))
            id_name = child.get("id")

            child_selector = f"{selector} > {tag_name}"
            if id_name:
                child_selector += f"#{id_name}"
            elif class_name:
                child_selector += f".{class_name}"

            child_selectors.add(child_selector)

        return child_selectors

    
    @staticmethod
    def get_child_selectors_with_selenium(url, selector, by=By.CSS_SELECTOR, driver=None, headless=False):
        """
        Extracts child element selectors from a parent element identified by a CSS selector using Selenium WebDriver.

        Parameters:
            url (str): The URL of the web page to scrape.
            selector (str): The CSS selector to identify the parent element.
            by (By, optional): The method of locating elements (default is By.CSS_SELECTOR).
            driver (WebDriver, optional): An existing Selenium WebDriver instance. If None, a new one is created.
            headless (bool, optional): If True, the browser runs in headless mode (default is False).

        Returns:
            set: A set of unique CSS selectors for the child elements of the parent element.
        """
        should_quit = False

        if driver is None:
            chrome_options = Options()
            if headless:
                chrome_options.add_argument("--headless")
            driver = webdriver.Chrome(options=chrome_options)
            should_quit = True

        result = set()

        try:
            driver.get(url)
            parent = driver.find_element(by, selector)
            if parent:
                child_elements = parent.find_elements(By.XPATH, "./*")
                child_selectors = set()

                for child in child_elements:
                    tag_name = child.tag_name
                    class_name = ".".join(child.get_attribute("class").split()) if child.get_attribute("class") else ""
                    id_name = child.get_attribute("id")

                    child_selector = f"{selector} > {tag_name}"
                    if id_name:
                        child_selector += f"#{id_name}"
                    elif class_name:
                        child_selector += f".{class_name}"

                    child_selectors.add(child_selector)

                result = child_selectors

        except Exception as e:
            print(f"Error: {e}")
        finally:
            if should_quit:
                driver.quit()

        return result

    
    @staticmethod
    def get_page_source_selenium(url, headless=True):
        """
        Fetches the page source of a website using Selenium.
    
        :param url: The URL of the website
        :param headless: Whether to run the browser in headless mode
        :return: The page source (HTML) as a string
        """
        options = Options()
        if headless:
            options.add_argument("--headless")  # Run in headless mode (no GUI)

        # Initialize the WebDriver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
        try:
            driver.get(url)
            page_source = driver.page_source  # Get the HTML source code
        finally:
            driver.quit()  # Close the browser

        return page_source
    
    @staticmethod
    def get_child_selectors_by_tag(page_source, parent_selector, child_tag, child_class=None):
        """
        The function of extracting the selector of the sub -attribute from a parent attribute
        Args:
            page_source (str): Source HTML of the website.
            parent_selector (str): Selector of Cha.
            child_tag (str): The HTML card I need to take (for example: 'Li', 'A', ...).
            child_class (str): (No required) Class of the sub -attribute.

        Returns:
        soup = BeautifulSoup(page_source, HTML_PARSER)
        """
        soup = BeautifulSoup(page_source, 'html.parser')
        parent_element = soup.select_one(parent_selector)
        if not parent_element:
            raise ValueError("The father's element was not found with Selector.")
    
        if child_class:
            child_elements = parent_element.find_all(child_tag, class_=child_class)
        else:
            child_elements = parent_element.find_all(child_tag)
    
        selectors = []
        for element in child_elements:
            selector = f"{child_tag}"
            if 'class' in element.attrs:
                class_name = "." + ".".join(element['class'])
                selector += class_name
            selectors.append(selector)
    
        return selectors
    
    @staticmethod
    def check_selector_exists(page_source, selector):
        """
        Check if Selector exists on the website.

        Args:
            page_source (str): Source HTML of the website.
            selector (str): Selector needs to check.

        Returns:
            bool: True if Selector exists, False otherwise.
        """
        soup = BeautifulSoup(page_source, 'html.parser')
        element = soup.select_one(selector)
        return element is not None
    
    @staticmethod
    def get_table_data_from_source(page_source, table_selector):
        """
        Extract data from an HTML table using page_source and a CSS selector.
        Parameters:
        - page_source (str): The HTML content of the page (page source).
        - table_selector (str): CSS selector to identify the table (e.g., 'table#myTable' or 'table.className').

        Returns:
        - list: A list of rows, where each row is a list of cell data.
        """
        try:
            # HTML analysis by beautyksoup
            soup = BeautifulSoup(page_source, 'html.parser')
        
            # Find the table according to Selector
            table = soup.select_one(table_selector)
            if not table:
                return {"Error: Can't find the board with Selector given."}
        
            # Get all rows (rows) in the table
            rows = table.find_all('tr')
            table_data = []
        
            # Browse through each row
            for row in rows:
                # Take all cells (cells) in rows
                cells = row.find_all(['td', 'th'])  # Including TH title
                # Get the text content of each cell, eliminate excess spaces
                row_data = [cell.get_text(strip=True) for cell in cells]
                if row_data:  # Only add goods if there is data
                    table_data.append(row_data)
                
            return table_data
    
        except Exception as e:
            return {"Error": f"Error when processing data: {str (e)}"}

