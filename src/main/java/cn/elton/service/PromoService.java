package cn.elton.service;

import cn.elton.service.model.PromoModel;

public interface PromoService {
    PromoModel getPromoByItemId(Integer itemId);

    void punlishPromo(Integer promoId);
}
