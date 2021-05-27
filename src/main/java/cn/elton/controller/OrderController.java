package cn.elton.controller;

import cn.elton.error.BuinessException;
import cn.elton.error.EmBusinessError;
import cn.elton.mq.MqProducer;
import cn.elton.response.CommonReturnType;
import cn.elton.service.ItemService;
import cn.elton.service.OrderService;
import cn.elton.service.model.OrderModel;
import cn.elton.service.model.UserModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.util.StreamUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.sql.BatchUpdateException;

@Controller("order")
@RequestMapping("/order")
@CrossOrigin(allowCredentials = "true", allowedHeaders = "*")
public class OrderController extends BaseController{

    @Autowired
    private OrderService orderService;

    @Autowired
    private HttpServletRequest httpServletRequest;

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private MqProducer mqProducer;

    @Autowired
    private ItemService itemService;

    //封装下单请求
    @RequestMapping(value = "/createorder", method = {RequestMethod.POST}, consumes = {CONTENT_TYPE_FORMED})
    @ResponseBody
    public CommonReturnType createOrder(@RequestParam(name = "itemId") Integer itemId,
                                        @RequestParam(name = "amount") Integer amount,
                                        @RequestParam(name = "promoId", required = false) Integer promoId) throws BuinessException {
        String token = httpServletRequest.getParameterMap().get("token")[0];
        if(StringUtils.isEmpty(token)){
            throw new BuinessException(EmBusinessError.USER_NOT_LOGIN, "用户尚未登录");
        }
        UserModel userModel = (UserModel)redisTemplate.opsForValue().get(token);
        if(userModel == null){
            throw new BuinessException(EmBusinessError.USER_NOT_LOGIN, "用户尚未登录");
        }
        //Boolean isLogin = (Boolean) this.httpServletRequest.getSession().getAttribute("IS_LOGIN");
//        if (isLogin == null || !isLogin.booleanValue()) {
//            throw new BuinessException(EmBusinessError.USER_NOT_LOGIN, "用户尚未登录");
//        }
//        获取用户登录信息
//        UserModel userModel = (UserModel) this.httpServletRequest.getSession().getAttribute("LOGIN_USER");
//        OrderModel orderModel = orderService.createOrder(userModel.getId(), itemId, amount, promoId);

//        OrderModel orderModel = orderService.createOrder(null, itemId, amount, promoId);
        //加入库存流水状态
         String stockLogId = itemService.initStockLog(itemId , amount);

        if(!mqProducer.transactionAsyncReduceStock(userModel.getId(),promoId,itemId,amount,stockLogId)){
            throw new BuinessException(EmBusinessError.UNKNOWN_ERROR);
        }
        return CommonReturnType.create(null);
    }
}
