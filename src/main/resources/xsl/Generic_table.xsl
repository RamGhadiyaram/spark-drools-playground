<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.1" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fo="http://www.w3.org/1999/XSL/Format" exclude-result-prefixes="fo">
    <xsl:param name="reportHeader"/>

    <!--
    @author : Ram G
     Generic style sheet to display spark generated  xmls in table format.
    reportHeader -  report header which was passed from Jaxp
     -->

    <xsl:template match="ROWS">
        <fo:root xmlns:fo="http://www.w3.org/1999/XSL/Format">
            <fo:layout-master-set>
                <fo:simple-page-master master-name="simpleA4" page-height="29.7cm" page-width="21cm" margin-top="1cm"
                                       margin-bottom="1cm" margin-left="1cm" margin-right="1cm">
                    <fo:region-body region-name="xsl-region-body" margin-bottom=".5in" margin-top=".50in"/>
                    <fo:region-before region-name="xsl-region-before" extent="5in"/>
                    <fo:region-after region-name="xsl-region-after" extent=".5in"/>
                </fo:simple-page-master>
            </fo:layout-master-set>
            <fo:page-sequence master-reference="simpleA4">

                <fo:static-content flow-name="xsl-region-before">
                    <fo:block font-size="17pt" color="navy">
                        <fo:table width="10in">
                            <fo:table-column/>
                            <fo:table-column/>
                            <fo:table-body>
                                <fo:table-row>

                                    <fo:table-cell text-align="center">
                                        <fo:block text-align="center">
                                            <xsl:message>report header
                                                <xsl:value-of select="$reportHeader"/>
                                            </xsl:message>
                                            <xsl:value-of select="$reportHeader"/>
                                        </fo:block>

                                    </fo:table-cell>
                                    <fo:table-cell>
                                        <fo:block>
                                            <fo:external-graphic
                                                    src="Ram.png"
                                                    content-width="1in"/>
                                        </fo:block>
                                    </fo:table-cell>
                                </fo:table-row>
                            </fo:table-body>
                        </fo:table>
                    </fo:block>
                </fo:static-content>

                <!--                <fo:static-content flow-name="xsl-region-before">-->
                <!--                    <fo:block color="Navy" text-align="center" font-size="10" font-weight="bold" width="100%">Sales Report - -->
                <!--                        <fo:external-graphic src="url()"></fo:external-graphic>-->
                <!--                    </fo:block>-->
                <!--                </fo:static-content>-->
                <fo:static-content flow-name="xsl-region-after">
                    <fo:block text-align="center" color="navy">
                        Page
                        <fo:page-number/>
                    </fo:block>
                </fo:static-content>
                <fo:flow flow-name="xsl-region-body">
                    <fo:block margin-top="1cm" border-color="sky">
                        <fo:block font-size="10pt">
                            <fo:table table-layout="auto" border-collapse="separate"
                                      inline-progression-dimension="auto">
                                <fo:table-column column-width="30%"/>
                                <fo:table-column column-width="20%"/>
                                <fo:table-column column-width="20%"/>
                                <fo:table-header background-color="navy" color="white">
                                    <xsl:for-each select="./ROW[1]/*">
                                        <fo:table-cell>
                                            <fo:block font-weight="bold">
                                                <xsl:value-of select="local-name()"/>
                                            </fo:block>
                                        </fo:table-cell>
                                    </xsl:for-each>
                                </fo:table-header>
                                <fo:table-body>
                                    <xsl:apply-templates select="ROW"/>
                                </fo:table-body>
                            </fo:table>
                        </fo:block>
                    </fo:block>
                </fo:flow>
            </fo:page-sequence>
        </fo:root>
    </xsl:template>

    <!-- template to process row -->
    <xsl:template match="ROW">
        <xsl:variable name="backgroundcolor">
            <xsl:choose>
                <xsl:when test="position() mod 2 =0">#A7BFDE</xsl:when>
                <xsl:otherwise>#EDF2F8</xsl:otherwise>
            </xsl:choose>
        </xsl:variable>

        <xsl:variable name="localName">
            <xsl:for-each select="//ROW[1]/*">
                <xsl:element name="names">
                    <xsl:value-of select="local-name()"/>
                </xsl:element>
            </xsl:for-each>
        </xsl:variable>
        <xsl:message>
            <xsl:value-of select="$localName"/>
        </xsl:message>
        <fo:table-row border-bottom="solid 0.5pt" border-color="#273B49" background-color="{$backgroundcolor}">
            <xsl:for-each select="*">
                <fo:table-cell>
                    <fo:block>
                        <xsl:variable name="nameOfElement" select="node()"/>
                        <xsl:message terminate="no">
                            element selected :
                            <xsl:value-of select="$nameOfElement"/>
                        </xsl:message>
                        <xsl:value-of select="$nameOfElement"/>
                    </fo:block>
                </fo:table-cell>
            </xsl:for-each>
        </fo:table-row>
    </xsl:template>
</xsl:stylesheet>