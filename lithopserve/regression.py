import numpy as np

class LinearPredictor:
    def __init__(self, init_coeficient_f, init_intercept, acc_coeficient_i, acc_coeficient_f, acc_intercept,
                 price_per_second):
        self.init_coeficient_f = init_coeficient_f  # alpha_1
        self.init_intercept = init_intercept  # alpha_0
        self.acc_coeficient_i = acc_coeficient_i  # beta_2
        self.acc_coeficient_f = acc_coeficient_f  # beta_1
        self.acc_intercept = acc_intercept  # beta_3
        self.price_per_second = price_per_second

    def predict_price_per_request(self, num_functions, num_images):
        log_num_images = np.log(num_images)
        log_num_functions = np.log(num_functions)
        log_term_a = log_num_images * self.acc_coeficient_i + log_num_functions * self.acc_coeficient_f + self.acc_intercept
        term_a = np.exp(log_term_a)
        log_num_functions = np.log(num_functions)
        log_term_i = log_num_functions * self.init_coeficient_f + self.init_intercept
        term_i = np.exp(log_term_i)
        total = term_a + term_i
        price_per_batch = total * self.price_per_second / num_images
        return price_per_batch

    def predict_num_functions(self, price_limit, num_images, min_functions=1, max_functions=1000, step=1):
        # List from min_functions to max_functions
        num_functions_range = np.arange(min_functions, max_functions, step)
        prices = []
        for num_functions in num_functions_range:
            price = self.predict_price_per_request(num_functions, num_images)
            if price > price_limit:
                break
            prices.append(price)
        prices = np.array(prices)
        idx = np.where(prices <= price_limit)
        idx = idx[0]
        if idx.size:
            return int(max(num_functions_range[idx]))
        else:
            return 1

    def predict_price(self, num_functions, num_images):
        log_num_images = np.log(num_images)
        log_num_functions = np.log(num_functions)
        log_term_a = log_num_images * self.acc_coeficient_i + log_num_functions * self.acc_coeficient_f + self.acc_intercept
        term_a = np.exp(log_term_a)
        log_num_functions = np.log(num_functions)
        log_term_i = log_num_functions * self.init_coeficient_f + self.init_intercept
        term_i = np.exp(log_term_i)
        total = term_a + term_i
        price = total * self.price_per_second
        return price
