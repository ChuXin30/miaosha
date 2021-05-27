package cn.elton.service.impl;

import cn.elton.dao.ItemDOMapper;
import cn.elton.dao.ItemStockDOMapper;
import cn.elton.dao.StockLogDOMapper;
import cn.elton.dataobject.ItemDO;
import cn.elton.dataobject.ItemStockDO;
import cn.elton.dataobject.StockLogDO;
import cn.elton.error.BuinessException;
import cn.elton.error.EmBusinessError;
import cn.elton.mq.MqProducer;
import cn.elton.service.ItemService;
import cn.elton.service.PromoService;
import cn.elton.service.model.ItemModel;
import cn.elton.service.model.PromoModel;
import cn.elton.validator.ValidationResult;
import cn.elton.validator.ValidatorImpl;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class ItemServiceImpl implements ItemService {

    @Autowired
    private ValidatorImpl validator;

    @Autowired
    private ItemDOMapper itemDOMapper;

    @Autowired
    private ItemStockDOMapper itemStockDOMapper;

    @Autowired
    private PromoService promoService;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private MqProducer mqProducer;

    @Autowired
    private StockLogDOMapper stockLogDOMapper;

    private ItemDO converItemDOFromItemModel(ItemModel itemModel) {
        if (itemModel == null) {
            return null;
        }
        ItemDO itemDO = new ItemDO();
        BeanUtils.copyProperties(itemModel, itemDO);
        return itemDO;
    }

    private ItemStockDO convertItemStockDOFromItemModel(ItemModel itemModel) {
        if (itemModel == null) {
            return null;
        }
        ItemStockDO itemStockDO = new ItemStockDO();
        itemStockDO.setItemId(itemModel.getId());
        itemStockDO.setStock(itemModel.getStock());
        return itemStockDO;
    }

    @Override
    @Transactional
    public ItemModel createItem(ItemModel itemModel) throws BuinessException {
        //检验入参
        ValidationResult result = validator.validate(itemModel);
        if (result.isHasErrors()) {
            throw new BuinessException(EmBusinessError.PARAMETER_VALIDATION_ERROR, result.getErrMsg());
        }
        //转化itemModel->dataobject
        ItemDO itemDO = this.converItemDOFromItemModel(itemModel);

        //写入数据库
        itemDOMapper.insertSelective(itemDO);
        itemModel.setId(itemDO.getId());
        ItemStockDO itemStockDO = this.convertItemStockDOFromItemModel(itemModel);

        itemStockDOMapper.insertSelective(itemStockDO);
        //返回创建完成的对象

        return this.getItemById(itemModel.getId());
    }

    @Override
    public List<ItemModel> listItem() {
        List<ItemDO> itemDOList = itemDOMapper.listItem();
        List<ItemModel> itemModelList = itemDOList.stream().map(itemDO -> {
            ItemStockDO itemStockDO = itemStockDOMapper.selectByItemId(itemDO.getId());
            ItemModel itemModel = this.convertModelFromDataObject(itemDO, itemStockDO);
            return itemModel;
        }).collect(Collectors.toList());
        return itemModelList;
    }

    @Override
    public ItemModel getItemById(Integer id) {
        ItemDO itemDO = itemDOMapper.selectByPrimaryKey(id);
        if (itemDO == null) {
            return null;
        }
        //操作获得库存数量
        ItemStockDO itemStockDO = itemStockDOMapper.selectByItemId(itemDO.getId());

        //dataobject -> model
        ItemModel itemModel = convertModelFromDataObject(itemDO, itemStockDO);

        //获取活动商品信息
        PromoModel promoModel = promoService.getPromoByItemId(itemModel.getId());
        if (promoModel != null && promoModel.getStatus().intValue() != 3) {
            itemModel.setPromoModel(promoModel);
        }
        return itemModel;
    }

    @Override
    @Transactional
    public boolean decreaseStock(Integer itemId, Integer amount) throws BuinessException {
//        int affectedRow = itemStockDOMapper.decreaseStock(itemId, amount);

        long result =  redisTemplate.opsForValue().increment( "promo_item_stock_"+itemId , amount.intValue()*-1);
        if (result >= 0) {
            //更新库存成功
//            boolean myResult = mqProducer.asyncReduceStock(itemId , amount);
//            if(!myResult){
//                redisTemplate.opsForValue().increment( "promo_item_stock_"+itemId , amount.intValue());
//                return false;
//            }
            return true;
        } else {
            //更新库存失败
//            redisTemplate.opsForValue().increment( "promo_item_stock_"+itemId , amount.intValue());
            increaseStock(itemId,amount);
            return false;
        }
    }

    @Override
    public boolean increaseStock(Integer itemId, Integer amount) throws BuinessException {
        redisTemplate.opsForValue().increment( "promo_item_stock_"+itemId , amount.intValue());
        return true;
    }


    @Override
    @Transactional
    public void increaseSales(Integer itemId, Integer amount) throws BuinessException {
        itemDOMapper.increaseSales(itemId, amount);
    }

    @Override
    public ItemModel getItemByIdCache(Integer id) {
        ItemModel itemModel = (ItemModel) redisTemplate.opsForValue().get("item_validate_"+id);
        if(itemModel == null){
            itemModel = this.getItemById(id);
            redisTemplate.opsForValue().set("item_validate_"+id , itemModel);
            redisTemplate.expire("item_validate_"+id , 10 , TimeUnit.MINUTES );
        }
        return itemModel;
    }

    @Override
    public boolean asyncDecreaseStock(Integer itemId, Integer amount) {
        boolean myResult = mqProducer.asyncReduceStock(itemId , amount);
        return myResult;
    }

    @Override
    @Transactional
    public String initStockLog(Integer itemId, Integer amount) {
        StockLogDO stockLogDO = new StockLogDO();
        stockLogDO.setItemId(itemId);
        stockLogDO.setAmount(amount);
        stockLogDO.setStockLogId(UUID.randomUUID().toString().replace("-",""));
        stockLogDO.setStatus(1);

        stockLogDOMapper.insertSelective(stockLogDO);
        return  stockLogDO.getStockLogId();
    }

    private ItemModel convertModelFromDataObject(ItemDO itemDO, ItemStockDO itemStockDO) {
        ItemModel itemModel = new ItemModel();
        BeanUtils.copyProperties(itemDO, itemModel);
        itemModel.setStock(itemStockDO.getStock());
        return itemModel;
    }
}
