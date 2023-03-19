from pi_associates_library.kisvn.processor.kisvn_liquidity_processor import KisvnLiquidProcessor
from pi_associates_library.kisvn.processor.kisvn_liquidity_processor import KisvnEstimateLiquidProcessor
from pi_associates_library.kisvn.processor.kisvn_missing_processor import KisvnMissingProcessor
from pi_associates_library.kisvn.processor.kisvn_raw_processor import KisvnRawProcessor
from pi_associates_library.kisvn.processor.kisvn_scrape_processor import KisvnScrapeProcessor


class KisvnProcessorFactory:
    @staticmethod
    def create_processor(name, params):
        if name == 'raw':
            return KisvnRawProcessor(params)
        elif name == 'missing':
            return KisvnMissingProcessor(params)
        elif name == 'liquid':
            return KisvnLiquidProcessor(params)
        elif name == 'liquid-for-missing':
            return KisvnEstimateLiquidProcessor(params)
        elif name == 'scrape':
            return KisvnScrapeProcessor(params)
        else:
            raise NotImplementedError
