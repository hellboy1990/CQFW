#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 城区范围确定

import datetime
import xlwt
import time
import pandas as pd
import arcpy
import csv
from arcpy.sa import *
from save_ras import check_file
from arcpy import env
import json
import os


# 提取初始范围
def get_csfw(dltb, csfw, field_xz0):
    arcpy.Delete_management(csfw)
    where_clause = 'CZCSXM = %s or CZCSXM = %s or CZCSXM = %s or CZCSXM = %s' % ("'201'", "'201A'", "'202'", "'202A'")
    arcpy.Select_analysis(dltb, csfw, where_clause)
    arcpy.AddField_management(csfw, field_xz0, "DOUBLE")
    arcpy.CalculateField_management(csfw, field_xz0, 0, "PYTHON3")


# 给初始范围新建新增字段
def csfw_xz(csfw, field_xz, field_xz1):
    arcpy.AddField_management(csfw, field_xz, "DOUBLE")
    # 构建字段
    intable = csfw
    fieldname = field_xz
    arcpy.CalculateField_management(intable, fieldname, "!%s!" % field_xz1, "PYTHON3")


# 提取基准范围，适用于第一次迭代
def get_jzfw(csfw, field_jzfw, jzfw, i):
    arcpy.AddField_management(csfw, field_jzfw, "DOUBLE")
    # 构建字段
    intable = csfw
    fieldname = field_jzfw
    dlmc_out = ["铁路用地", "轨道交通用地", "公路用地", "城镇村道路用地", "管道运输用地", "沟渠"]
    expression = "jzfw_in_out(!DLMC!)"
    # 判断语句
    codeblock = """def jzfw_in_out(DLMC):
            if DLMC in %s:
                return 0
            else:
                return 1""" % dlmc_out
    arcpy.CalculateField_management(intable, fieldname, expression, "PYTHON3", codeblock)

    # 提取满足条件的初始范围作为基准范围
    infeature = csfw
    arcpy.MakeFeatureLayer_management(infeature, "csfw%s_layer" % i)  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(jzfw)
    expression_jzfw = ' "%s" = 1 ' % field_jzfw
    # print(expression_jzfw)
    arcpy.SelectLayerByAttribute_management("csfw%s_layer" % i, "NEW_SELECTION", expression_jzfw)
    arcpy.CopyFeatures_management("csfw%s_layer" % i, jzfw)


# 提取基准范围1，适用于第二次及以上迭代
def get_jzfw1(csfw, field_jzfw, field_xz, jzfw, i):
    arcpy.AddField_management(csfw, field_jzfw, "DOUBLE")
    # 构建字段
    intable = csfw
    fieldname = field_jzfw
    # 一类
    dlmc_out = ["铁路用地", "轨道交通用地", "公路用地", "城镇村道路用地", "管道运输用地", "沟渠", "干渠"]
    # 二类
    dlmc_out1 = ["沿海滩涂", "内陆滩涂", "沼泽地",
                 "乔木林地", "竹林地", "灌木林地", "其他林地",
                 "天然牧草地", "人工牧草地", "其他草地",
                 "河流水面", "湖泊水面", "水库水面", "坑塘水面", "养殖坑塘", "水工建筑用地", "冰川及永久积雪",
                 ]
    expression = "jzfw_in_out(!DLMC!, !%s!)" % field_xz
    # 判断语句
    codeblock = """def jzfw_in_out(DLMC, %s):
            if DLMC in %s:
                return 0
            elif %s > 0 and DLMC in %s:
                return 0
            else:
                return 1""" % (field_xz, dlmc_out, field_xz, dlmc_out1)
    arcpy.CalculateField_management(intable, fieldname, expression, "PYTHON3", codeblock)

    # 提取满足条件的初始范围作为基准范围
    infeature = csfw
    arcpy.MakeFeatureLayer_management(infeature, "csfw%s_layer" % i)  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(jzfw)
    expression_jzfw = ' "%s" = 1 ' % field_jzfw
    # print(expression_jzfw)
    arcpy.SelectLayerByAttribute_management("csfw%s_layer" % i, "NEW_SELECTION", expression_jzfw)
    arcpy.CopyFeatures_management("csfw%s_layer" % i, jzfw)


# 基准范围缓冲100米并融合字段
def jzfw_buffer(jzfw, jzfw_buf, fid="SJNF"):
    arcpy.Delete_management(jzfw_buf)
    arcpy.Buffer_analysis(jzfw, jzfw_buf, "100 Meters", "FULL", "ROUND", "LIST", fid)
    dropfields = [fid]
    arcpy.DeleteField_management(jzfw_buf, dropfields)


# 基准范围100米缓冲区分割
def jzfw_buffer_split(jzfw_buf, jzfw_buf_1):
    arcpy.Delete_management(jzfw_buf_1)
    arcpy.MultipartToSinglepart_management(jzfw_buf, jzfw_buf_1)


# 根据最小要素面积踢除独立斑块
def jzfw_area_limit(jzfw_buf_1, area_limit, jzfw_buf_2, jzfw_buf_3, i):
    infeature = jzfw_buf_1
    arcpy.MakeFeatureLayer_management(infeature, "buf%s_layer" % i)  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(jzfw_buf_2)
    expression_limit = ' "Shape_Area" >= %s ' % area_limit
    arcpy.SelectLayerByAttribute_management("buf%s_layer" % i, "NEW_SELECTION", expression_limit)
    arcpy.CopyFeatures_management("buf%s_layer" % i, jzfw_buf_2)

    # 融合
    arcpy.Delete_management(jzfw_buf_3)
    arcpy.Dissolve_management(jzfw_buf_2, jzfw_buf_3, ["ORIG_FID"], "", "MULTI_PART", "")


# 原始图斑与基准范围的缓冲区拼接
def dltb_j_jzfw_buf(dltb, jzfw_buf, stfw_lx_tmp1):
    arcpy.Delete_management(stfw_lx_tmp1)
    fieldmappings = arcpy.FieldMappings()
    # fieldmap = fieldmappings.getFieldMap(jzfw_buf)
    # print(fieldmap)
    fieldmappings.addTable(dltb)  # 目标图层
    fieldmappings.addTable(jzfw_buf)  # 合并图层
    arcpy.SpatialJoin_analysis(dltb, jzfw_buf, stfw_lx_tmp1,
                               join_operation="JOIN_ONE_TO_ONE", join_type=False,
                               field_mapping=fieldmappings, match_option="INTERSECT", )


# 链接必选与候选的规则
def dltb_j_bx_hx(stfw_lx_tmp1, rule_bx_hx, stfw_lx_tmp2, i):
    infeature = stfw_lx_tmp1
    infeature1 = stfw_lx_tmp2
    jointable = rule_bx_hx
    input_joinfield = "DLMC"
    out_joinfield = "ERLEI"
    arcpy.MakeFeatureLayer_management(infeature, "tmp1%s_layer" % i)  # 要素转为layer
    arcpy.AddJoin_management("tmp1%s_layer" % i, input_joinfield, jointable, out_joinfield)
    arcpy.Delete_management(infeature1)
    arcpy.CopyFeatures_management("tmp1%s_layer" % i, infeature1)


# 判断必选与候选,1必选0候选2排除
def dltb_bx_hx(stfw_lx_tmp2, field_bx_hx, name_bx_hx):
    # 新建字段存储必选与候选字段
    arcpy.AddField_management(stfw_lx_tmp2, field_bx_hx, "DOUBLE")
    # 构建字段
    field_min_area = str(name_bx_hx).replace(".csv", "_csv") + "_MIN_AREA_m2_"
    field_in_out = str(name_bx_hx).replace(".csv", "_csv") + "_IN_OUT"
    intable = stfw_lx_tmp2
    fieldname = field_bx_hx
    expression = "get_bx_hx(!Shape_Area!, !%s!, !%s!)" % (field_min_area, field_in_out)
    # 判断语句
    codeblock = """def get_bx_hx(area, min_area, in_out):
        if area >= int(min_area):
            return in_out
        else:
            pass"""
    arcpy.CalculateField_management(intable, fieldname, expression, "PYTHON3", codeblock)

    # 踢除字段
    fields = arcpy.ListFields(stfw_lx_tmp2)
    # print(fields)
    fields_list = [i.name for i in fields]
    # print(fields_list)
    fields_list1 = ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area', field_bx_hx]
    for i in fields_list1:
        fields_list.remove(i)
    # print(fields_list)
    arcpy.DeleteField_management(stfw_lx_tmp2, fields_list)


# 提取必选与候选范围
def dltb_to_bx_hx(stfw_lx_tmp2, field_bx_hx, bx, hx, i):
    infeature = stfw_lx_tmp2
    arcpy.MakeFeatureLayer_management(infeature, "tmp2%s_layer" % i)  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(bx)
    expression_bx = ' "%s" = 1 ' % field_bx_hx
    # print(expression_bx)
    arcpy.SelectLayerByAttribute_management("tmp2%s_layer" % i, "NEW_SELECTION", expression_bx)
    arcpy.CopyFeatures_management("tmp2%s_layer" % i, bx)
    # 提取候选
    arcpy.Delete_management(hx)
    expression_hx = ' "%s" = 0 ' % field_bx_hx
    arcpy.SelectLayerByAttribute_management("tmp2%s_layer" % i, "NEW_SELECTION", expression_hx)
    arcpy.CopyFeatures_management("tmp2%s_layer" % i, hx)


# 从必选与候选范围中减掉初始范围
def bx_hx_csfw(bx, hx, jzfw, bx_n, hx_n, field_sx):
    arcpy.Delete_management(bx_n)
    arcpy.Delete_management(hx_n)
    arcpy.Erase_analysis(bx, jzfw, bx_n)
    arcpy.Erase_analysis(hx, jzfw, hx_n)
    # 给候选要素添加筛选字段
    arcpy.AddField_management(hx_n, field_sx, "DOUBLE")
    arcpy.CalculateField_management(hx_n, field_sx, 1, "PYTHON3")


# 候选要素筛选
def hx_re(hx_n, dltb, csfw, field_sx, dlmcs1, hx_n1, hx_n2, field_bx_hx):
    # 计算距离
    arcpy.Near_analysis(hx_n, csfw)

    # 相交以获取地类
    arcpy.Delete_management(hx_n1)
    infeatures = [hx_n, dltb]
    arcpy.Intersect_analysis(infeatures, hx_n1, "ALL")

    intable = hx_n1
    fieldname = field_sx
    expression = "get_hx_re(!DLMC!, !NEAR_DIST!)"
    # 判断语句
    codeblock = """def get_hx_re(DLMC, DIST):
            if DLMC in %s:
                return 0
            else:
                if DIST == 0:
                    return 1
                else:
                    return 0""" % dlmcs1
    arcpy.CalculateField_management(intable, fieldname, expression, "PYTHON3", codeblock)

    # 提取要素
    arcpy.Delete_management(hx_n2)
    where_clause = "%s = 1" % field_sx
    arcpy.Select_analysis(hx_n1, hx_n2, where_clause)
    # 踢除字段
    fields = arcpy.ListFields(hx_n2)
    # print(fields)
    fields_list = [i.name for i in fields]
    # print(fields_list)
    fields_list1 = ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area', field_bx_hx]
    for i in fields_list1:
        fields_list.remove(i)
    arcpy.DeleteField_management(hx_n2, fields_list)


# 合并必选与候选要素
def dltb_merge(bx_n, hx_n1, bx_hx_n):
    arcpy.Delete_management(bx_hx_n)
    arcpy.Merge_management([bx_n, hx_n1], bx_hx_n)


# 必选&候选与缓冲区拼接
def bx_hx_lianjie(bx_hx_n, zuge, zuge_buf, stfw_lj_tmp1, stfw_lj_tmp2, filed_lj, fid="SJNF",):
    # 缓冲区50米
    arcpy.Delete_management(zuge_buf)
    arcpy.Buffer_analysis(zuge, zuge_buf, "50 Meters", "FULL", "ROUND", "LIST", fid)
    dropfields = [fid]
    arcpy.DeleteField_management(zuge_buf, dropfields)

    # 将必选/候选要素与缓冲区拼接
    dltb_j_jzfw_buf(bx_hx_n, zuge_buf, stfw_lj_tmp1)
    # 给在影响范围内的必选/候选要素添加连接字段
    arcpy.AddField_management(stfw_lj_tmp1, filed_lj, "DOUBLE")
    arcpy.CalculateField_management(stfw_lj_tmp1, filed_lj, 1, "PYTHON3")

    # 提取不在影响范围内的必选/候选要素
    arcpy.Delete_management(stfw_lj_tmp2)
    arcpy.Erase_analysis(bx_hx_n, stfw_lj_tmp1, stfw_lj_tmp2)
    # 给不在影响范围内的必选/候选要素添加连接字段
    arcpy.AddField_management(stfw_lj_tmp2, filed_lj, "DOUBLE")
    arcpy.CalculateField_management(stfw_lj_tmp2, filed_lj, 1, "PYTHON3")


# 判断缓冲区内必选&候选区与基准范围的连接关系
def bx_hx_lianjie_re():
    pass


# 给新增范围添加标签
def xz_bx_hx(stfw_bx_hx_i, field_xz):
    arcpy.AddField_management(stfw_bx_hx_i, field_xz)
    # 构建字段
    intable = stfw_bx_hx_i
    fieldname = field_xz
    expression = 1
    arcpy.CalculateField_management(intable, fieldname, expression, "PYTHON3")

    # 踢除字段
    fields = arcpy.ListFields(stfw_bx_hx_i)
    # print(fields)
    fields_list = [i.name for i in fields]
    # print(fields_list)
    fields_list1 = ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area', field_xz]
    for i in fields_list1:
        fields_list.remove(i)
    # print(fields_list)
    arcpy.DeleteField_management(stfw_bx_hx_i, fields_list)


# 不在阻隔要素影响内的+连接的+初始范围
def bx_hx_lj_csfw(stfw_lj_tmp2, bx_hx_n1, csfw, stfw_i):
    arcpy.Delete_management(stfw_i)
    arcpy.Merge_management([stfw_lj_tmp2, bx_hx_n1, csfw], stfw_i)


# 更新要素
def dltb_union(stfw_bx_hx_i, csfw, stfw_i):
    arcpy.Delete_management(stfw_i)
    arcpy.Update_analysis(stfw_bx_hx_i, csfw, stfw_i, "NO_BORDERS")


def stfw_dltb(stfw_i, dltb, stfw_i1, field_xz):
    arcpy.Delete_management(stfw_i1)
    infeatures = [stfw_i, dltb]
    arcpy.Intersect_analysis(infeatures, stfw_i1, "ALL")
    # 删除字段
    fields = arcpy.ListFields(stfw_i1)
    # print(fields)
    fields_list = [i.name for i in fields]
    # print(fields_list)
    fields_list1 = ['OBJECTID', 'Shape', 'Shape_Length', 'Shape_Area', field_xz, "DLMC", "SJNF"]
    for i in fields_list1:
        fields_list.remove(i)
    # print(fields_list)
    arcpy.DeleteField_management(stfw_i1, fields_list)


def main():
    time_start = datetime.datetime.now()
    print('开始时间:' + str(time_start))
    print("------------------------城区范围确定------------------------")

    # 设置工作环境
    configs = open(".\\config_cqfw.json")
    configs_datas = json.load(configs)["cqfw"]
    env.workspace = configs_datas["user_worksapce"]
    arcpy.env.nodata = 'NONE'
    arcpy.CheckExtension('spatial')

    # 设置数据
    dltb = configs_datas["dltb"]  # 迭代一的初始范围
    bx_hx = configs_datas["bx_hx"]  # 识别必选与候选的规则
    zuge = configs_datas["zuge"]
    print("“基础数据”:%s; “必选与候选规则”:%s; “阻隔要素”: %s"
          % (dltb, bx_hx, zuge))

    # 提取初始范围
    csfw = "CSFW"
    # csfw = "D1_STFW_DLTB2020"
    field_xz0 = "XZ0"  # 标识原始范围
    get_csfw(dltb, csfw, field_xz0)
    print("已确定初始范围:%s" % csfw)

    i = 1
    while i < 4:  # 迭代3次
        try:
            # 基准范围是指参与迭代的范围，其中铁路用地、公路用地等线头要素不参与迭代
            jzfw = "D%s_JZFW" % i
            field_jzfw = "DD%s" % i
            field_xz = "XZ%s" % i  # 新增要素的识别标准
            field_xz1 = "XZ%s" % (i-1)
            csfw_xz(csfw, field_xz, field_xz1)  # 给初始范围添加标签
            if i == 1:
                get_jzfw(csfw, field_jzfw, jzfw, i)
            else:
                get_jzfw1(csfw, field_jzfw, field_xz, jzfw, i)
            print("第%s次迭代！已确定基准范围:%s" % (i, jzfw))
            # 初始范围100米缓冲区
            jzfw_buf = jzfw + "_BUF100"
            jzfw_buffer(jzfw, jzfw_buf)
            print("第%s次迭代！已确定基准范围的100m缓冲区范围:%s" % (i, jzfw_buf))
            # 100米缓冲区分割,以踢除独立在外的小斑块
            jzfw_buf_1 = jzfw_buf + "_1"
            jzfw_buf_2 = jzfw_buf + "_2"
            jzfw_buf_3 = jzfw_buf + "_3"
            jzfw_buffer_split(jzfw_buf, jzfw_buf_1)
            area_limit = int(input("请输入参与迭代的最小要素面积:"))
            jzfw_area_limit(jzfw_buf_1, area_limit, jzfw_buf_2, jzfw_buf_3, i)

            # 拼接
            stfw_lx_tmp1 = dltb + "_J_" + jzfw_buf
            dltb_j_jzfw_buf(dltb, jzfw_buf_3, stfw_lx_tmp1)
            print("第%s次迭代！已确定100m缓冲区与%s的相交范围:%s" % (i, dltb, stfw_lx_tmp1))

            # 判断必选与候选
            stfw_lx_tmp2 = "D%s" % i + "_" + dltb + "_J_" + "BX_HX"
            field_bx_hx = "D%s_BX_HX" % i  # 迭代1
            name_bx_hx = os.path.split(bx_hx)[1]
            dltb_j_bx_hx(stfw_lx_tmp1, bx_hx, stfw_lx_tmp2, i)  # 首先进行规则链接
            dltb_bx_hx(stfw_lx_tmp2, field_bx_hx, name_bx_hx)  # 判断必选与候选
            print("第%s次迭代！已确定必选与候选要素的范围:%s" % (i, stfw_lx_tmp2))

            # 提取必选与候选
            bx, hx = "D%s_BX" % i, "D%s_HX" % i
            bx_n, hx_n = bx + "_N", hx + "_N"
            dltb_to_bx_hx(stfw_lx_tmp2, field_bx_hx, bx, hx, i)
            field_sx = "SX%s" % i
            bx_hx_csfw(bx, hx, csfw, bx_n, hx_n, field_sx)  # 踢除初始范围,只保留新增范围
            print("第%s次迭代！已提取出必选与候选要素的范围:%s, %s, %s, %s" % (i, bx, hx, bx_n, hx_n))

            # 候选要素的进一步筛选
            hx_n1 = hx_n + "1"
            hx_n2 = hx_n + "2"
            dlmcs1 = ["沿海滩涂", "内陆滩涂", "沼泽地",
                      "天然牧草地",
                      "水库水面", "冰川及永久积雪"]  # ，一票否决类：湿地、天然草地、冰川生态敏感；水库、面积过大
            dlmcs2 = ["乔木林地", "竹林地", "灌木林地", "其他林地",
                      "人工牧草地", "其他草地",
                      "河流水面", "湖泊水面", "水库水面", "坑塘水面", "养殖坑塘", "水工建筑用地",
                      "城镇村道路用地", "管道运输用地"]  # 其它是以距离判断类
            hx_re(hx_n, dltb, csfw, field_sx, dlmcs1, hx_n1, hx_n2, field_bx_hx)  # 候选要素筛选与提取
            Q1 = int(input("请检查候选要素是否满足要求，如果是请输入1，否请输入其它任意数字！："))
            if Q1 == 1:
                hx_n3 = hx_n2
            else:
                hx_n3 = str(input("请输入进一步筛选后的候选要素！"))
            print("第%s次迭代！已完成对候选要素的筛选!" % i)

            # 必选与候选合并
            bx_hx_n = "D%s_BX_HX" % i
            dltb_merge(bx_n, hx_n3, bx_hx_n)
            # 删除筛选字段
            dropfields = [field_sx]
            arcpy.DeleteField_management(bx_hx_n, dropfields)

            '''连接条件判断'''
            zuge_buf = zuge + "_BUF50"
            stfw_lj_tmp1 = bx_hx_n + "_J_" + zuge_buf
            stfw_lj_tmp2 = bx_hx_n + "_LJ"
            field_lj = "LIAN%s" % i
            bx_hx_lianjie(bx_hx_n, zuge, zuge_buf, stfw_lj_tmp1, stfw_lj_tmp2, field_lj)
            print("第%s次迭代！已确定在阻隔要素影响范围内、不在阻隔要素影响范围内的必选与候选要素范围:%s, %s"
                  % (i, stfw_lj_tmp1, stfw_lj_tmp2))

            # 进一步判断
            bx_hx_lianjie_re()
            print("第%s次迭代！请对连接条件做出进一步判断！" % i)
            bx_hx_n1 = str(input("请输入连接条件判断后的要素名称:"))

            # 将连接条件判断后要素与初始范围合并
            stfw_i = "D%s_STFW" % i
            stfw_bx_hx_i = "D%s_XZ_STFW" % i
            dltb_merge(stfw_lj_tmp2, bx_hx_n1, stfw_bx_hx_i)  # 连接判断后要素+连接要素
            xz_bx_hx(stfw_bx_hx_i, field_xz)
            print("第%s次迭代！已确定新增实体地域范围:%s" % (i, stfw_bx_hx_i))
            # 新增要素+初始范围要素
            dltb_union(stfw_bx_hx_i, csfw, stfw_i)
            print("第%s次迭代！已确定更新后实体地域范围：%s" % (i, stfw_i))
            # 与DLTB相交以获得DLMC等信息
            stfw_i1 = stfw_i + "_" + dltb
            stfw_dltb(stfw_i, dltb, stfw_i1, field_xz)

            # 更新迭代的初始范围
            csfw = stfw_i1
            print("第%s次迭代结束！已更新初始范围为%s！" % (i, csfw))
        except Exception as err:
            print(err.args[0])

        i += 1

    time_end = datetime.datetime.now()
    print('结束时间:' + str(time_start))
    time_last = time_end - time_start
    print('共耗时:' + str(time_last))


main()