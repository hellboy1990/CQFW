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
def get_csfw(dltb, csfw):
    arcpy.Delete_management(csfw)
    where_clause = 'CZCSXM = %s or CZCSXM = %s or CZCSXM = %s or CZCSXM = %s' % ("'201'", "'201A'", "'202'", "'202A'")
    arcpy.Select_analysis(dltb, csfw, where_clause)


# 提取基准范围
def get_jzfw(csfw, field_jzfw, jzfw):
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
    arcpy.MakeFeatureLayer_management(infeature, "csfw_layer")  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(jzfw)
    expression_jzfw = ' "%s" = 1 ' % field_jzfw
    # print(expression_jzfw)
    arcpy.SelectLayerByAttribute_management("csfw_layer", "NEW_SELECTION", expression_jzfw)
    arcpy.CopyFeatures_management("csfw_layer", jzfw)


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
def jzfw_area_limit(jzfw_buf_1, area_limit, jzfw_buf_2, jzfw_buf_3):
    infeature = jzfw_buf_1
    arcpy.MakeFeatureLayer_management(infeature, "buf_layer")  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(jzfw_buf_2)
    expression_limit = ' "Shape_Area" >= %s ' % area_limit
    arcpy.SelectLayerByAttribute_management("buf_layer", "NEW_SELECTION", expression_limit)
    arcpy.CopyFeatures_management("buf_layer", jzfw_buf_2)

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
def dltb_j_bx_hx(stfw_lx_tmp1, rule_bx_hx, stfw_lx_tmp2):
    infeature = stfw_lx_tmp1
    infeature1 = stfw_lx_tmp2
    jointable = rule_bx_hx
    input_joinfield = "DLMC"
    out_joinfield = "ERLEI"
    arcpy.MakeFeatureLayer_management(infeature, "tmp1_layer")  # 要素转为layer
    arcpy.AddJoin_management("tmp1_layer", input_joinfield, jointable, out_joinfield)
    arcpy.Delete_management(infeature1)
    arcpy.CopyFeatures_management("tmp1_layer", infeature1)


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


# 提取必选与候选范围
def dltb_to_bx_hx(stfw_lx_tmp2, field_bx_hx, bx, hx):
    infeature = stfw_lx_tmp2
    arcpy.MakeFeatureLayer_management(infeature, "tmp2_layer")  # 要素转为layer
    # 提取必选
    arcpy.Delete_management(bx)
    expression_bx = ' "%s" = 1 ' % field_bx_hx
    # print(expression_bx)
    arcpy.SelectLayerByAttribute_management("tmp2_layer", "NEW_SELECTION", expression_bx)
    arcpy.CopyFeatures_management("tmp2_layer", bx)
    # 提取候选
    arcpy.Delete_management(hx)
    expression_hx = ' "%s" = 0 ' % field_bx_hx
    arcpy.SelectLayerByAttribute_management("tmp2_layer", "NEW_SELECTION", expression_hx)
    arcpy.CopyFeatures_management("tmp2_layer", hx)


# 从必选与候选范围中减掉初始范围
def bx_hx_csfw(bx, hx, jzfw, bx_n, hx_n):
    arcpy.Delete_management(bx_n)
    arcpy.Delete_management(hx_n)
    arcpy.Erase_analysis(bx, jzfw, bx_n)
    arcpy.Erase_analysis(hx, jzfw, hx_n)


# 候选要素筛选
# def hx_re(hx, field_hxn, hxn):
#     arcpy.AddField_management(hx, field_hxn, "DOUBLE")
#     intable = hx
#     fieldname = field_hxn
#     expression = "get_hx_re(!Shape_Area!, !%s!, !%s!)" % (field_min_area, field_in_out)
#     # 判断语句
#     codeblock = """def get_hx_re(area, min_area, in_out):
#             if area >= int(min_area):
#                 return in_out
#             else:
#                 pass"""
#     arcpy.CalculateField_management(intable, fieldname, expression, "PYTHON3", codeblock)
#     infeature = hx
#     arcpy.MakeFeatureLayer_management(infeature, "hx_layer")  # 要素转为layer
#
#     arcpy.Delete_management(hxn)
#     expression_hx_re = ' "%s" = 0 ' % field_bx_hx
#     arcpy.SelectLayerByAttribute_management("hx_layer", "NEW_SELECTION", expression_hx_re)
#     arcpy.CopyFeatures_management("hx_layer", hx)


# 连接条件判断


# 合并必选与候选要素
def dltb_merge(bx_n, hx_n1, bx_hx_n):
    arcpy.Delete_management(bx_hx_n)
    arcpy.Merge_management([bx_n, hx_n1], bx_hx_n)


def bx_hx_lianjie(bx_hx_n, zuge, zuge_buf, stfw_lj_tmp1, stfw_lj_tmp2, fid="SJNF",):
    # 缓冲区50米
    arcpy.Delete_management(zuge_buf)
    arcpy.Buffer_analysis(zuge, zuge_buf, "50 Meters", "FULL", "ROUND", "LIST", fid)
    dropfields = [fid]
    arcpy.DeleteField_management(zuge_buf, dropfields)

    # 将必选/候选要素与缓冲区拼接
    dltb_j_jzfw_buf(bx_hx_n, zuge_buf, stfw_lj_tmp1)
    # 提取不在影响范围内的必选/候选要素
    arcpy.Delete_management(stfw_lj_tmp2)
    arcpy.Erase_analysis(bx_hx_n, stfw_lj_tmp1, stfw_lj_tmp2)


# 不在阻隔要素影响内的+连接的+初始范围
def bx_hx_lj_csfw(stfw_lj_tmp2, bx_hx_n1, csfw, stfw_i):
    try:
        arcpy.Delete_management(stfw_i)
    except:
        pass
    arcpy.Merge_management([stfw_lj_tmp2, bx_hx_n1, csfw], stfw_i)


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
    # area_limit = configs_datas["area_limit"]  # 参与迭代的最小要素面积
    zuge = configs_datas["zuge"]
    print("------------------------“基础数据”:%s; “必选与候选规则”:%s; “阻隔要素”: %s------------------------"
          % (dltb, bx_hx, zuge))

    # 提取初始范围
    csfw = "CSFW"
    get_csfw(dltb, csfw)
    print("------------------------已确定初始范围！------------------------")

    i = 1
    while i < 2:  # 迭代3次
        try:
            # 基准范围是指参与迭代的范围，其中铁路用地、公路用地等线头要素不参与迭代
            jzfw = "D%s_JZFW" % i
            field_jzfw = "DD%s" % i
            get_jzfw(csfw, field_jzfw, jzfw)
            print("------------------------第%s次迭代！已确定基准范围！------------------------" % i)

            # 初始范围100米缓冲区
            jzfw_buf = jzfw + "_BUF100"
            jzfw_buffer(jzfw, jzfw_buf)
            print("------------------------第%s次迭代！已确定基准范围的100m缓冲区范围！------------------------" % i)
            # 100米缓冲区分割,以踢除独立在外的小斑块
            jzfw_buf_1 = jzfw_buf + "_1"
            jzfw_buf_2 = jzfw_buf + "_2"
            jzfw_buf_3 = jzfw_buf + "_3"
            jzfw_buffer_split(jzfw_buf, jzfw_buf_1)
            area_limit = int(input("请输入参与迭代的最小要素面积:"))
            jzfw_area_limit(jzfw_buf_1, area_limit, jzfw_buf_2, jzfw_buf_3)

            # 拼接
            stfw_lx_tmp1 = dltb + "_J_" + jzfw_buf
            dltb_j_jzfw_buf(dltb, jzfw_buf, stfw_lx_tmp1)
            print("------------------------第%s次迭代！已确定100m缓冲区与%s的相交范围！------------------------" % (i, dltb))

            # 判断必选与候选
            stfw_lx_tmp2 = "D%s" % i + dltb + "_J_" + "BX_HX"
            field_bx_hx = "D%s_BX_HX" % i  # 迭代1
            name_bx_hx = os.path.split(bx_hx)[1]
            dltb_j_bx_hx(stfw_lx_tmp1, bx_hx, stfw_lx_tmp2)  # 首先进行规则链接
            dltb_bx_hx(stfw_lx_tmp2, field_bx_hx, name_bx_hx)  # 判断必选与候选
            print("------------------------第%s次迭代！已确定必选与候选要素的范围！------------------------" % i)

            # 提取必选与候选
            bx, hx = "D%s_BX" % i, "D%s_HX" % i
            bx_n, hx_n = bx + "_N", hx + "_N"
            dltb_to_bx_hx(stfw_lx_tmp2, field_bx_hx, bx, hx)
            bx_hx_csfw(bx, hx, csfw, bx_n, hx_n)  # 踢除初始范围,只保留新增范围
            print("------------------------第%s次迭代！已提取出必选与候选要素的范围！------------------------" % i)

            # 候选要素的进一步筛选
            field_hxn = "D%s_HX_re" % i
            # hx_n1 = hx_n + "_re"
            # hx_re(hx, field_hxn, hxn)
            print("------------------------第%s次迭代！请对候选要素进一步筛选！------------------------" % i)
            hx_n1 = str(input("请输入筛选后的候选要素名称:"))
            # 必选与候选合并
            bx_hx_n = "D%s_BX_HX" % i
            dltb_merge(bx_n, hx_n1, bx_hx_n)

            '''连接条件判断'''
            zuge_buf = zuge + "_BUF50"
            stfw_lj_tmp1 = bx_hx_n + "_J_" + zuge_buf
            stfw_lj_tmp2 = bx_hx_n + "_LJ"
            bx_hx_lianjie(bx_hx_n, zuge, zuge_buf, stfw_lj_tmp1, stfw_lj_tmp2)
            print("------------------------第%s次迭代！已确定在阻隔要素影响范围内的必选与候选要素范围！------------------------" % i)

            # 进一步判断
            print("------------------------第%s次迭代！请对连接条件做出进一步判断！------------------------" % i)
            bx_hx_n1 = str(input("请输入连接条件判断后的要素名称:"))

            # 将连接条件判断后要素与初始范围合并
            stfw_i = "D%s_STFW" % i
            bx_hx_lj_csfw(stfw_lj_tmp2, bx_hx_n1, csfw, stfw_i)

            # 更新迭代的初始范围
            csfw = stfw_i
            print("------------------------第%s次迭代结束！已更新初始范围为%s！------------------------" % (i, csfw))
        except Exception as err:
            print(err.args[0])

        i += 1

    time_end = datetime.datetime.now()
    print('结束时间:' + str(time_start))
    time_last = time_end - time_start
    print('共耗时:' + str(time_last))

main()