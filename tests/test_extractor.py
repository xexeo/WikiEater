import unittest

from wikieater.extractor import clean_and_extract, normalize_internal_url


class ExtractorTests(unittest.TestCase):
    def test_normalize_internal_url_filters_special_file_and_external(self):
        base = "https://example.fandom.com/wiki/Main_Page"
        self.assertEqual(
            normalize_internal_url(base, "/wiki/Item_One#section"),
            "https://example.fandom.com/wiki/Item_One",
        )
        self.assertIsNone(normalize_internal_url(base, "https://google.com/wiki/A"))
        self.assertIsNone(normalize_internal_url(base, "/wiki/Special:Random"))
        self.assertIsNone(normalize_internal_url(base, "/wiki/File:Icon.png"))

    def test_clean_and_extract_removes_script_img_and_keeps_classes(self):
        source = """
        <html><body>
            <div class=\"mw-parser-output\"><h1 class=\"title\">Sword</h1>
                <p class=\"description\">Strong item.</p>
                <a href=\"/wiki/Category:Weapons\">Weapons</a>
                <img src=\"ad.png\" />
                <script>alert(1)</script>
            </div>
        </body></html>
        """
        cleaned, links = clean_and_extract(source, "https://example.fandom.com/wiki/Sword")
        self.assertIn("Strong item.", cleaned)
        self.assertIn('class="description"', cleaned)
        self.assertIn("page-category", cleaned)
        self.assertNotIn("alert(1)", cleaned)
        self.assertNotIn("img", cleaned)
        self.assertIn("/wiki/Category:Weapons", links)


if __name__ == "__main__":
    unittest.main()
