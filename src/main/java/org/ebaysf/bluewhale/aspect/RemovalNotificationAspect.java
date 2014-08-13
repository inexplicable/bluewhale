package org.ebaysf.bluewhale.aspect;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.ebaysf.bluewhale.util.RemovalNotificationValueHolder;

/**
 * Created by huzhou on 5/6/14.
 */
@Aspect
public class RemovalNotificationAspect {

    @Around("execution(* com.google.common.cache.RemovalNotification.getValue())")
    public Object aroundGetValue(final ProceedingJoinPoint pjp) throws Throwable {

        final Object value = pjp.proceed();

        if(value instanceof RemovalNotificationValueHolder){
            return ((RemovalNotificationValueHolder)value).getValue();
        }
        return value;
    }

}
